package io.warp10.storm.ext;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class StormWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions = new HashMap<String, Object>();
  
  static {
    functions.put("MQTTSPOUT", new MQTTSPOUT("MQTTSPOUT"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
