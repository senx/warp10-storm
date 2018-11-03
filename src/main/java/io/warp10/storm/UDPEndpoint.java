//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.storm;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Charsets;

public class UDPEndpoint extends Thread {
  
  private final DatagramSocket socket;
  
  /**
   * Queue to buffer the incoming messages
   */
  private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1024);

  private AtomicBoolean abort = new AtomicBoolean(false);
  
  public UDPEndpoint(String host, int port) throws IOException {
    this.socket = new DatagramSocket(port, InetAddress.getByName(host));
  }
  
  @Override
  public void run() {
    byte[] buf = new byte[65536];
    DatagramPacket packet = new DatagramPacket(buf, buf.length);
    while(!this.abort.get()) {      
      try {
        this.socket.receive(packet);
        
        String data = new String(packet.getData(), packet.getOffset(), packet.getLength(), Charsets.UTF_8);
        
        
      } catch (Exception e) {        
      }
    }
    
    this.socket.close();
  }
  
  public void close() {
    this.abort.set(true);
    this.interrupt();
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
