package io.warp10.storm.ext;

import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class MQTTSPOUT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
  public static final String KEY_STREAMID = "streamid";
  public static final String KEY_HOST = "host";
  public static final String KEY_USER = "user";
  public static final String KEY_PASSWORD = "password";
  public static final String KEY_topics = "topics";

  public MQTTSPOUT(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a map on top of the stack.");     
    }
    
    Map<String,Object> map = (Map<String,Object>) top;
    
    stack.push(new StormMqttSpout(map.get(KEY_STREAMID).toString(), map.get(KEY_HOST).toString(), map.get(KEY_USER).toString(), map.get(KEY_PASSWORD).toString(), (List<String>) map.get(KEY_topics)));

    return stack;
  }  
}
