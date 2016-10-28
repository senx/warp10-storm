package io.warp10.storm;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.script.WarpScriptLib;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.LockSupport;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class WarpScriptTopology {
  
  private static final String WARP10_CONFIG = "warp10.config";
  
  public WarpScriptTopology(String topologyName, List<String> files) throws Exception {
        
    List<WarpScriptNode> nodes = new ArrayList<WarpScriptNode>();
    
    for (String file: files) {
      //BufferedReader br = new BufferedReader(new FileReader(file));
      InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      
      StringBuilder sb = new StringBuilder();
      
      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        sb.append(line);
        sb.append("\n");
      }
      
      br.close();
      
      //WarpScriptNode bolt = new WarpScriptNode(sb.toString());
      List<WarpScriptNode> subnodes = WarpScriptNode.parse(sb.toString());
      
      nodes.addAll(subnodes);
      
      //nodes.add(bolt);
    }

    System.out.println("Defining topology with " + nodes.size() + " nodes.");
    
    TopologyBuilder tb = new TopologyBuilder();
    
    for (WarpScriptNode node: nodes) {
      if (node.isBolt()) {
        BoltDeclarer bd = tb.setBolt(node.getId(), node, node.getParallelism());
        bd.setMaxTaskParallelism(node.getParallelism());
        bd.setNumTasks(node.getParallelism());
        bd.setDebug(true);
        node.declare(bd);
      } else if (node.isSpout()) {
        SpoutDeclarer sd = tb.setSpout(node.getId(), node, node.getParallelism());
        sd.setMaxTaskParallelism(node.getParallelism());
        sd.setNumTasks(node.getParallelism());
        sd.setDebug(true);
      }      
    }

    StormTopology topology = tb.createTopology();

    Config conf = new Config();
    
    boolean local = false;
    
    Map<String,String> env = new HashMap<String,String>(System.getenv());
    for (Entry<Object,Object> entry: System.getProperties().entrySet()) {
      env.put(entry.getKey().toString(), entry.getValue().toString());
    }
    
    conf.setEnvironment(env);
    //conf.setClasspath(System.getenv("CLASSPATH"));
    
    if (local) {
      LocalCluster cluster = new LocalCluster(conf);

      cluster.submitTopology(topologyName, conf, topology);
    } else {
      conf.setNumWorkers(20);
      conf.setMaxSpoutPending(5000);
      StormSubmitter.submitTopology(topologyName, conf, topology);
    }           
  }
  
  public static void main(String... args) throws Exception {

    System.out.println("LAUNCHING TOPOLOGY");
    
    String topologyName = args[0];
    
    List<String> files = new ArrayList<String>();

    for (int i = 1; i < args.length; i++) {
      files.add(args[i]);
    }
    
    if (null != System.getProperty(WARP10_CONFIG)) {      
      WarpConfig.setProperties(new InputStreamReader(WarpScriptTopology.class.getResourceAsStream(System.getProperty(WARP10_CONFIG))));
    } else {
      if (null == System.getProperty(Configuration.WARP_TIME_UNITS)) {
        System.setProperty(Configuration.WARP_TIME_UNITS, "us");
      }
      
      System.setProperty(Configuration.WARPSCRIPT_REXEC_ENABLE, "true");      
      
      WarpConfig.setProperties((String) null); 
    }
    
    WarpScriptLib.registerExtensions();
    
    WarpScriptTopology topology = new WarpScriptTopology(topologyName, files);

    LockSupport.parkNanos(Long.MAX_VALUE);
  }
}
