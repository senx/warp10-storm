package io.warp10.storm;

import org.junit.Test;

public class TopologyTest {
  @Test
  public void testSubmit() throws Exception {
    WarpScriptTopology.main("test", "/Users/hbs/workspace/warp10-flink/spout.mc2", "/Users/hbs/workspace/warp10-flink/bolt.mc2");
  }
}
