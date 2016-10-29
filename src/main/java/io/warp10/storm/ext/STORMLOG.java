package io.warp10.storm.ext;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class STORMLOG extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public STORMLOG(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();        
    System.out.println(Thread.currentThread() + " TOP=" + top.toString());        
    return stack;
  }
}
