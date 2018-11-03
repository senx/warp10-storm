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

import java.util.List;
import java.util.Map;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class MQTTSPOUT extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
  public static final String MQTTSPOUT = "MQTTSPOUT";
  
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
