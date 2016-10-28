package io.warp10.storm;

import java.util.HashMap;
import java.util.Map;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.WarpScriptExtension;

public class StormWarpScriptExtension extends WarpScriptExtension {
  
  private static final WarpScriptExtension singleton = new StormWarpScriptExtension();

  private static Map<String,Object> functions = new HashMap<String,Object>();
  
  static {
    functions.put("_storm.LOG", new WarpScriptStackFunction() {
      @Override
      public Object apply(WarpScriptStack stack) throws WarpScriptException {
        Object top = stack.pop();        
        System.out.println(Thread.currentThread() + " TOP=" + top.toString());        
        return stack;
      }
      
      @Override
      public String toString() {
        return "_storm.LOG";
      }
    });    
  }
  
  public static WarpScriptExtension getInstance() {
    return singleton;
  }  
  
  @Override
  public Map<String, Object> getFunctions() {    
    return functions;
  }
}
