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
package io.warp10.storm.ext;

import io.warp10.script.WarpScriptStack;
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;

import java.net.URLEncoder;
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

public class StormMqttSpout implements IRichSpout, Snapshotable {
  
  private TopologyContext context = null;
  private Map<String,Object> conf = null;
  private SpoutOutputCollector collector = null;
  
  private final String password;
  private final String userName;
  private final String host;
  
  private BlockingConnection connection = null;
  
  private final String streamId;
  
  private final Topic[] topics;
  
  private final List<String> subscriptions;
  
  public StormMqttSpout(String streamId, String host, String userName, String password, List<String> subscriptions) {
    this.streamId = streamId;
    this.host = host;
    this.userName = userName;
    this.password = password;
    this.subscriptions = subscriptions;
    
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
      if (null != userName) {
        mqtt.setUserName(userName);
      }
      if (null != password) {
        mqtt.setPassword(password);
      }
      
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
  
  
  @Override
  public String snapshot() {
    StringBuilder sb = new StringBuilder();

    try {
      sb.append("{ ");
      SNAPSHOT.addElement(sb, MQTTSPOUT.KEY_HOST);
      SNAPSHOT.addElement(sb, this.host);
      SNAPSHOT.addElement(sb, MQTTSPOUT.KEY_USER);
      SNAPSHOT.addElement(sb, this.userName);
      SNAPSHOT.addElement(sb, MQTTSPOUT.KEY_PASSWORD);
      SNAPSHOT.addElement(sb, this.password);
      SNAPSHOT.addElement(sb, MQTTSPOUT.KEY_STREAMID);
      SNAPSHOT.addElement(sb, this.streamId);
      SNAPSHOT.addElement(sb, MQTTSPOUT.KEY_topics);
      SNAPSHOT.addElement(sb, this.subscriptions);
      sb.append("} ");
      sb.append(MQTTSPOUT.MQTTSPOUT);
      sb.append(" ");
    } catch (Exception e) {      
    }
    return sb.toString();
  }
}
