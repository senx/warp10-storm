package io.warp10.storm.ext;

import io.warp10.warp.sdk.WarpScriptExtension;

import java.util.HashMap;
import java.util.Map;

public class StormDefaultWarpScriptExtension extends WarpScriptExtension {
  
  private static final WarpScriptExtension singleton = new StormDefaultWarpScriptExtension();

  private static Map<String,Object> functions = new HashMap<String,Object>();
  
  static {
    functions.put("_storm.LOG", new STORMLOG("_storm.LOG"));  
  }
  
  public static WarpScriptExtension getInstance() {
    return singleton;
  }  
  
  @Override
  public Map<String, Object> getFunctions() {    
    return functions;
  }
}
