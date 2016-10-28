package io.warp10.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

@WebSocket
public class PlasmaConsumer extends Thread {

  private final AtomicBoolean abort = new AtomicBoolean(false);
  
  private final String endpoint;
  
  private CountDownLatch closeLatch;

  private Session session = null;

  private final AtomicBoolean done = new AtomicBoolean(false);

  private final String token;
  
  private final String[] selectors;
  
  private final long period;
  
  private AtomicLong count = new AtomicLong(0L);
  
  /**
   * Queue to buffer the incoming messages
   */
  private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1024);
  
  public PlasmaConsumer(String endpoint, String token, long period, String... selectors) {    
    this.endpoint = endpoint;
    this.token = token;
    this.selectors = selectors;
    this.period = period;
  }

  public void close() {
    this.abort.set(true);
  }
    
  @Override
  public void run() {    
    while(true) {
      WebSocketClient client = null;
      
      try {
        SslContextFactory ssl = new SslContextFactory();
        client = new WebSocketClient(ssl);
        client.start();
        URI uri = new URI(this.endpoint);
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        
        this.closeLatch = new CountDownLatch(1);
        this.session = null;
        
        Future<Session> future = client.connect(this, uri, request);
        
        //
        // Wait until we're connected to the endpoint
        //
        
        while(!future.isDone()) {
          LockSupport.parkNanos(250000000L);
        }

        //
        // Wait until we're told to exit or the socket has closed
        //
        
        long lastSub = 0L;

        this.session.getRemote().sendString("WRAPPER");
        
        while(true) {
          //
          // Every now and then re-issue 'SUBSCRIBE' statements to the endpoint
          //
          if (null == this.session || this.done.get()) {
            return;
          }
          
          if (System.currentTimeMillis() - lastSub > this.period) {
            for (String selector: selectors) {
              this.session.getRemote().sendString("SUBSCRIBE " + this.token + " " + selector);
            }
            lastSub = System.currentTimeMillis();
          }
          
          LockSupport.parkNanos(1000000000L);
        }
      } catch (Throwable t) {
        t.printStackTrace();
        try { this.awaitClose(1, TimeUnit.SECONDS); } catch (InterruptedException ie) {}
      } finally {
        try { client.stop(); } catch (Throwable t) {}
      }
    }
  }

  public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
    return this.closeLatch.await(duration, unit);
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    this.session = null;
    this.closeLatch.countDown();
  }
 
  @OnWebSocketError
  public void onError(Throwable cause) {
    cause.printStackTrace();
  }
  
  @OnWebSocketConnect
  public void onConnect(Session session) {
    this.session = session;
  }
 
  @OnWebSocketMessage
  public void onMessage(String msg) {
    
    BufferedReader br = null;
    try {
      br = new BufferedReader(new StringReader(msg));      
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }

        try {
          this.queue.put(line);
        } catch (Exception e) {
          // Ignore...
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      if (null != br) {
        try {
          br.close();
        } catch (Exception e) {          
        }
      }
    }
  }
  
  public String getNext(long timeout, TimeUnit unit) {
    if (timeout < 0) {
      return this.queue.poll();
    } else {
      try {
        return this.queue.poll(timeout, unit);
      } catch (InterruptedException ie) {
        return null;
      }
    }
  }
}
