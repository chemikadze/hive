package org.apache.hadoop.hive.ql.parse.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.util.LinkedList;
import java.util.List;

public class BeamTask extends Task<BeamWork> {

  public BeamTask(BeamWork work) {
    setWork(work);
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "BEAM";
  }

  @Override
  protected int execute(DriverContext driverContext) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    List<String> dummyData = new LinkedList<>();
    dummyData.add("one");
    dummyData.add("two");
    dummyData.add("three");

    Combine.Globally<String, Long> countStrings = Combine.globally(Count.combineFn());

    PCollection<Long> countPipeline = p
        .apply(Create.of(dummyData))
        .apply(countStrings);

    p.run().waitUntilFinish();

    return 42;
  }

}
