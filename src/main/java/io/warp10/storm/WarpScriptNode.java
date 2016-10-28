package io.warp10.storm;

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptLib;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.functions.SNAPSHOT;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarpScriptNode implements IRichBolt, IRichSpout, Serializable {

  public static final Logger LOG = LoggerFactory.getLogger(WarpScriptNode.class);
  
  public static final String KEY_CLASS = "class";
  
  public static final String KEY_DEBUG = "debug";
  public static final String KEY_TYPE = "type";
  public static final String KEY_ID = "id";
  public static final String KEY_MACRO = "macro";
  public static final String KEY_OUTPUT = "output";
  public static final String KEY_INPUT = "input";
  public static final String KEY_PARALLELISM = "parallelism";
  public static final String KEY_EVERY = "every";
  public static final String KEY_TOKEN = "token";
  public static final String KEY_SELECTORS = "selectors";
  public static final String KEY_PLASMA_ENDPOINT = "plasma.endpoint";
  public static final String KEY_UDP_HOST = "udp.host";
  public static final String KEY_UDP_PORT = "udp.port";
  
  private WarpScriptStack stack;
  private String id;
  private Macro macro;
  
  private SpoutOutputCollector spoutCollector = null;
  private OutputCollector boltCollector = null;
  private boolean bolt = false;
  private boolean spout = false;
  
  private Map<Object,Object> inputs;
  private Map<Object,Object> outputs;

  private String mc2;
  
  private int parallelism = 1;
  
  private long every = Long.MAX_VALUE;
  private long lasttuple;
  
  private PlasmaConsumer plasmaConsumer;
  
  private UDPEndpoint udpEndpoint;
  
  private boolean debug = false;
  
  private TopologyContext context = null;

  /**
   * Is this node a simple wrapper around a spout/bolt?
   */
  private boolean wrapped = false;
  
  private IRichSpout wrappedSpout = null;
  private IRichBolt wrappedBolt = null;
  
  public WarpScriptNode() {
    initStack();
  }
  
  public WarpScriptNode(String code) throws WarpScriptException {
    this();
        
    //
    // Execute the bolt code, this code should leave on the stack a map with the following fields:
    //
    // {
    //   'id' <The id of this bolt>
    //   'macro' <The macro which will be executed for each tuple>
    //   'output' <A map of stream id to list of fields> 
    //   'input' <A map of { 'component' { 'stream' 'grouping' } }>
    // }
    //
          
    if (null != System.getProperty("debug")) {
      System.err.println("Initializing node with " + code);
    }
    init(code);
  }
  
  private static WarpScriptStack stackInit() {
    Properties properties = new Properties();
    
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
    stack.maxLimits();
    
    WarpScriptLib.register(StormWarpScriptExtension.getInstance());
    return stack;
  }
  
  public static List<WarpScriptNode> parse(String code) throws WarpScriptException {
    WarpScriptStack stack = stackInit();
    
    stack.execMulti(code);
    
    List<Object> levels = new ArrayList<Object>();

    while(stack.depth() > 0) {
      levels.add(stack.pop());
    }
    
    List<WarpScriptNode> nodes = new ArrayList<WarpScriptNode>();
    
    //
    // We snapshot the symbols also
    //
    
    SNAPSHOT snapshot = new SNAPSHOT("", true, false);
    
    for (Object level: levels) {      
      stack.push(level);
      snapshot.apply(stack);
      nodes.add(new WarpScriptNode(stack.pop().toString()));
    }
    
    return nodes;
  }
  
  private void init(String code) throws WarpScriptException {
    
    this.mc2 = code;

    stack.execMulti(code);
    
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException("Node code should leave a map on top of the stack.");
    }
    
    Map<String,Object> config = (Map<String,Object>) top;
    
    if (!config.containsKey(KEY_TYPE)) {
      throw new WarpScriptException("Node should declare its type in the '" + KEY_TYPE + "' field.");
    }
    
    if ("bolt".equals(config.get(KEY_TYPE))) {
      this.bolt = true;
      this.spout = false;
    } else if ("spout".equals(config.get(KEY_TYPE))) {
      this.bolt = false;
      this.spout = true;
    } else {
      throw new WarpScriptException("Invalid node type.");
    }

    if (!config.containsKey(KEY_ID)) {
      throw new WarpScriptException("Node should declare its id in the '" + KEY_ID + "' field.");
    }
    
    this.id = config.get(KEY_ID).toString();
    
    if (config.containsKey(KEY_PARALLELISM)) {
      this.parallelism = Integer.parseInt(config.get(KEY_PARALLELISM).toString());
    } else {
      this.parallelism = 1;
    }
    
    if (config.containsKey(KEY_DEBUG)) {
      this.debug = Boolean.TRUE.equals(config.get(KEY_DEBUG));
    }

    //
    // If the 'class' param is defined we assume it is an instance of
    // a bolt or spout created by a specific function and use it
    // as the wrapped component.
    //
    
    if (config.containsKey(KEY_CLASS)) {
      this.wrapped = true;
      
      try {
        if (this.bolt) {
          this.wrappedBolt = (IRichBolt) config.get(KEY_CLASS);
        } else {
          this.wrappedSpout = (IRichSpout) config.get(KEY_CLASS);
        }
      } catch (Exception e) {
        throw new WarpScriptException(e);
      }      
    }
    
    if (!wrapped && (!config.containsKey(KEY_MACRO) || !(config.get(KEY_MACRO) instanceof Macro))) {
      throw new WarpScriptException("Node should declare its macro in the '" + KEY_MACRO + "' field.");
    }
    
    this.macro = (Macro) config.get(KEY_MACRO);
    
    //
    // Input is a map of { 'component' { 'stream' 'grouping' } } specs
    //
    
    if (this.bolt && (!config.containsKey(KEY_INPUT) || !(config.get(KEY_INPUT) instanceof Map))) {
      throw new WarpScriptException("Bolt should declare its inputs in the '" + KEY_INPUT + "' field.");
    } else if (this.bolt) {
      this.inputs = (Map<Object,Object>) config.get(KEY_INPUT);
    }
        
    if (!wrapped && (!config.containsKey(KEY_OUTPUT) || !(config.get(KEY_OUTPUT) instanceof Map))) {
      throw new WarpScriptException("Node should declare its outputs in the '" + KEY_OUTPUT + "' field.");
    }
    
    this.outputs = (Map<Object,Object>) config.get(KEY_OUTPUT);

    if (this.spout && config.containsKey(KEY_EVERY)) {
      this.every = Long.parseLong(config.get(KEY_EVERY).toString());
    }
    
    if (!wrapped && config.containsKey(KEY_PLASMA_ENDPOINT)
        && config.containsKey(KEY_TOKEN)
        && config.containsKey(KEY_SELECTORS)
        && config.containsKey(KEY_EVERY)) {      
      Object[] oselectors = ((List<Object>) config.get(KEY_SELECTORS)).toArray();
      String[] selectors = new String[oselectors.length];
      for (int i = 0; i < oselectors.length; i++) {
        selectors[i] = oselectors[i].toString();        
      }
      
      this.plasmaConsumer = new PlasmaConsumer(config.get(KEY_PLASMA_ENDPOINT).toString(),
          config.get(KEY_TOKEN).toString(),
          ((Number) config.get(KEY_EVERY)).longValue(),
          selectors);
    } else if (!wrapped && config.containsKey(KEY_UDP_PORT) && config.containsKey(KEY_UDP_HOST)) {
      try {
        this.udpEndpoint = new UDPEndpoint(config.get(KEY_UDP_HOST).toString(), Integer.parseInt(config.get(KEY_UDP_PORT).toString()));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
  
  private void initStack() {
    this.stack = stackInit();
  }
  
  public String getId() {
    return this.id;
  }
  
  public int getParallelism() {
    return this.parallelism;
  }
  
  @Override
  public void cleanup() {
    if (null != this.plasmaConsumer) {
      this.plasmaConsumer.close();
    }
  }
  
  public void declare(BoltDeclarer bd) {
    
    if (!this.bolt) {
      throw new RuntimeException("Node '" + this.id + "' is not a bolt.");
    }

    for (Entry<Object, Object> input: this.inputs.entrySet()) {
      
      if (!(input.getValue() instanceof Map)) {
        throw new RuntimeException("Input specifier is invalid, should be a map of streamId to grouping.");
      }
      
      String componentId = input.getKey().toString();
      
      for (Entry<Object,Object> entry: ((Map<Object,Object>) input.getValue()).entrySet()) {
        String streamId = entry.getKey().toString();
        String grouping = entry.getValue().toString();

        if ("shuffle".equals(grouping)) {
          bd.shuffleGrouping(componentId, streamId);
        } else if ("none".equals(grouping)) {
          bd.noneGrouping(componentId, streamId);
        } else if ("global".equals(grouping)) {
          bd.globalGrouping(componentId, streamId);
        } else if ("local".equals(grouping)) {
          bd.localOrShuffleGrouping(componentId, streamId);
        } else if ("all".equals(grouping)) {
          bd.allGrouping(componentId, streamId);
        } else if (grouping.startsWith("fields:")) {
          String[] sfields = grouping.substring(7).split(",");
          Fields fields = new Fields(Arrays.asList(sfields));
          bd.fieldsGrouping(componentId, streamId, fields);        
        }
      }
    }
  }
  
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    if (wrapped) {
      if (null != wrappedSpout) {
        this.wrappedSpout.declareOutputFields(declarer);
      } else if (null != wrappedBolt) {
        this.wrappedBolt.declareOutputFields(declarer);
      }
      return;
    }
    //
    // Loop on the outputs and declare them
    //
    
    for (Entry<Object,Object> output: this.outputs.entrySet()) {
      
      if (!(output.getValue() instanceof List)) {
        throw new RuntimeException("Invalid output specifier, should be 'streamiId fieldlist'.");
      }
      
      String streamId = output.getKey().toString();
      
      List<String> fields = new ArrayList<String>();
      for (Object field: ((List<Object>) output.getValue())) {
        fields.add(field.toString());
      }
      declarer.declareStream(streamId, new Fields(fields));
    }
  }
  
  @Override
  public Map<String, Object> getComponentConfiguration() {
    if (wrapped) {
      if (null != wrappedSpout) {
        return this.wrappedSpout.getComponentConfiguration();
      } else if (null != wrappedBolt) {
        return this.wrappedBolt.getComponentConfiguration();
      }
    }

    return new HashMap<String,Object>();
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    if (!this.bolt) {
      throw new RuntimeException("Node '" + this.id + "' is not a bolt.");
    }

    if (wrapped) {
      this.wrappedBolt.prepare(stormConf, context, collector);
      return;
    }

    this.context = context;
    this.boltCollector = collector;
  }
  
  @Override
  public void execute(Tuple input) {
    
    if (!this.bolt) {
      throw new RuntimeException("Node '" + this.id + "' is not a bolt.");
    }
    
    if (wrapped) {
      this.wrappedBolt.execute(input);
      return;
    }
    
    Map<String,Object> tuple = new HashMap<String,Object>();
    
    try {
      this.stack.push(new WarpScriptStack.Mark());
      this.stack.push(input.getSourceTask());
      this.stack.push(input.getSourceComponent());
      this.stack.push(input.getSourceStreamId());
      //this.stack.push(input.getSourceGlobalStreamid().toString());
      this.stack.push(input.getMessageId().toString());
      
      for (int i = 0; i < input.size(); i++) {
        tuple.put(input.getFields().get(i), input.getValue(i));
      }

      this.stack.push(tuple);

      if (debug) {
        System.err.println("PRE " + this.id + " [" + this.context.getThisTaskId() + "]");
        System.err.println(stack.dump(Integer.MAX_VALUE));
      }

      this.stack.exec(this.macro);

      if (debug) {
        System.err.println("POST " + this.id + " [" + this.context.getThisTaskId() + "]");
        System.err.println(stack.dump(Integer.MAX_VALUE));
      }

      if (0 < this.stack.depth()) {
        //
        // Now treat the output (a map)
        //
        
        Object top = this.stack.pop();
        
        if (!(top instanceof Map)) {
          throw new RuntimeException("Invalid bolt output, should have been a map.");
        }
        
        Map<Object,Object> output = (Map<Object,Object>) top;
        
        //
        // Key is output stream, value is a list of list of field values
        //
      
        for (Entry<Object,Object> entry: output.entrySet()) {
          String streamId = entry.getKey().toString();
          
          if (!(entry.getValue() instanceof List)) {
            throw new RuntimeException("Invalid bolt output, should have been a map of streamId to list of tuples.");
          }
                    
          for (Object elt: (List<Object>) entry.getValue()) {
            this.boltCollector.emit(streamId, (List<Object>) elt);
          }
        }
      }
            
      //
      // Ack the input tuple
      //
      
      this.boltCollector.ack(input);
    } catch (Throwable t) {
      t.printStackTrace();
      throw new RuntimeException(t);
    }
    
    
  }
  
  public boolean isBolt() {
    return this.bolt;
  }
  
  public boolean isSpout() {
    return this.spout;
  }
  
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    Properties props = WarpConfig.getProperties();
    
    StringBuilder sb = new StringBuilder();
    
    for (Entry<Object, Object> entry: props.entrySet()) {
      sb.append(URLEncoder.encode(entry.getKey().toString(), "UTF-8").replaceAll("\\+", "%20"));
      sb.append("=");
      sb.append(URLEncoder.encode(entry.getValue().toString(), "UTF-8").replaceAll("\\+", "%20"));
      sb.append("\n");
    }
    
    out.writeUTF(sb.toString());
    out.writeUTF(this.mc2);
  }
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    String properties = in.readUTF();
    
    StringReader sw = new StringReader(properties);
    
    synchronized (WarpConfig.class) {
      if(!WarpConfig.isPropertiesSet()) {
        WarpConfig.setProperties(sw);
      }
    }
    
    String code = in.readUTF();
    try {
      initStack();
      init(code);    
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }
  
  //
  // IRichSpout
  //
  
  @Override
  public void ack(Object msgId) {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    
    if (wrapped) {
      this.wrappedSpout.ack(msgId);
      return;
    }
  }

  @Override
  public void activate() {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    if (wrapped) {
      this.wrappedSpout.activate();
      return;
    }
    if (null != this.plasmaConsumer) {
      this.plasmaConsumer.start();
    }
    if (null != this.udpEndpoint) {
      this.udpEndpoint.start();
    }
  }
  
  @Override
  public void close() {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    if (wrapped) {
      this.wrappedSpout.close();
      return;
    }
    if (null != this.plasmaConsumer) {
      this.plasmaConsumer.close();
    }
    if (null != this.udpEndpoint) {
      this.udpEndpoint.close();
    }
  }
  
  @Override
  public void deactivate() {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    if (wrapped) {
      this.wrappedSpout.deactivate();
      return;
    }
  }
  
  @Override
  public void fail(Object msgId) {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    if (wrapped) {
      this.wrappedSpout.fail(msgId);
      return;
    }
  }
  
  @Override
  public void nextTuple() {
    if (!this.spout) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }

    if (wrapped) {
      this.wrappedSpout.nextTuple();
      return;
    }
    
    //
    // Sleep if it's not time for the next tuple yet
    //
    
    if (null != this.plasmaConsumer) {
      String wrapper = this.plasmaConsumer.getNext(1000, TimeUnit.MILLISECONDS);
      if (null == wrapper) {
        return;
      }
      
      try {
        this.stack.push(wrapper);
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    } else if (null != this.udpEndpoint) {
      String packet = this.udpEndpoint.getNext(1000, TimeUnit.MILLISECONDS);
      if (null == packet) {
        return;
      }
      try {
        this.stack.push(packet);
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    } else {
      if (this.every > 0) {
        long now = System.currentTimeMillis();
        if (now - lasttuple < this.every) {
          LockSupport.parkNanos(1000000L * (this.every - (now - lasttuple)));
        }
        lasttuple = System.currentTimeMillis();
      }            
    }
    
    try {
      
      if (debug) {
        System.err.println("PRE " + this.id + " [" + this.context.getThisTaskId() + "]");
        System.err.println(stack.dump(Integer.MAX_VALUE));
      }

      this.stack.exec(this.macro);

      if (debug) {
        System.err.println("POST " + this.id + " [" + this.context.getThisTaskId() + "]");
        System.err.println(stack.dump(Integer.MAX_VALUE));
      }
      
      if (0 < this.stack.depth()) {
        //
        // Now treat the output (a map)
        //
        
        Object top = this.stack.pop();
        
        if (!(top instanceof Map)) {
          throw new RuntimeException("Invalid bolt output, should have been a map.");
        }
        
        Map<Object,Object> output = (Map<Object,Object>) top;
        
        //
        // Key is output stream, value is a list of list of field values
        //
      
        for (Entry<Object,Object> entry: output.entrySet()) {
          String streamId = entry.getKey().toString();
          
          if (!(entry.getValue() instanceof List)) {
            throw new RuntimeException("Invalid bolt output, should have been a map of streamId to list of tuples.");
          }

          for (Object elt: (List<Object>) entry.getValue()) {
            this.spoutCollector.emit(streamId, (List<Object>) elt);
          }
        }
      }
    } catch (WarpScriptException wse) {
      LOG.error("Error in nextTuple()", wse);
    } catch (Throwable t) {
      t.printStackTrace();
      throw new RuntimeException(t);
    }
  }
  
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    if (this.bolt) {
      throw new RuntimeException("Node '" + this.id + "' is not a spout.");
    }
    
    if (wrapped) {
      this.wrappedSpout.open(conf, context, collector);
      return;
    }
    
    this.context = context;
    this.spoutCollector = collector;
  }  
}