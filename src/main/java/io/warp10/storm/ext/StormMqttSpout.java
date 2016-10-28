package io.warp10.storm.ext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class StormMqttSpout implements IRichSpout {
  
  private TopologyContext context = null;
  private Map<String,Object> conf = null;
  private SpoutOutputCollector collector = null;
  
  private final String password;
  private final String userName;
  private final String host;
  
  private BlockingConnection connection = null;
  
  private final String streamId;
  
  private final Topic[] topics;
  
  public StormMqttSpout(String streamId, String host, String userName, String password, List<String> subscriptions) {
    this.streamId = streamId;
    this.host = host;
    this.userName = userName;
    this.password = password;
    this.topics = new Topic[subscriptions.size()];
    
    for (int i = 0; i < topics.length; i++) {
      String subscription = subscriptions.get(i);
      String[] subtokens = subscription.split("\\s+");
      
      topics[i] = new Topic(subtokens[0], QoS.valueOf(subtokens[1]));      
    }
  }
  
  @Override
  public void ack(Object msgId) {
  }
  
  @Override
  public void activate() {
    MQTT mqtt = new MQTT();
    try {
      mqtt.setHost(host);      
      mqtt.setUserName(userName);
      mqtt.setPassword(password);
      
      this.connection = mqtt.blockingConnection();
      this.connection.connect();
      
      this.connection.subscribe(topics);      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void deactivate() {
    try {
      this.connection.disconnect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void close() {
    deactivate();
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    Fields fields = new Fields("topic", "payload");
    declarer.declareStream(streamId, fields);
  }
  
  @Override
  public void fail(Object msgId) {
  }
  
  @Override
  public Map<String, Object> getComponentConfiguration() {
    return this.conf;
  }
  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.conf = conf;
    this.context = context;
    this.collector = collector;
  }
  
  @Override
  public void nextTuple() {
    try {
      Message msg = this.connection.receive();
      List<Object> tuple = new ArrayList<Object>();
      
      tuple.add(msg.getTopic());
      tuple.add(msg.getPayload());
      
      collector.emit(streamId, tuple);
      msg.ack();
    } catch (Exception e) {
      return;
    }    
  }
}
