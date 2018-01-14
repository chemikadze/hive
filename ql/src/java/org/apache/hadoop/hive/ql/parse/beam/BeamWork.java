package org.apache.hadoop.hive.ql.parse.beam;

import org.apache.hadoop.hive.ql.plan.AbstractOperatorDesc;
import org.apache.hadoop.hive.ql.plan.Explain;

import java.util.HashMap;
import java.util.Map;

@Explain(displayName = "Beam", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class BeamWork extends AbstractOperatorDesc {

  @Explain(displayName = "DummyExplainField")
  public Map<String, String> getName() {
    Map<String, String> result = new HashMap<>();
    result.put("X", "one");
    result.put("Y", "two");
    return result;
  }

}
