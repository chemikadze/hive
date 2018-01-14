package org.apache.hadoop.hive.ql.parse.beam;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.MergeJoinProc;
import org.apache.hadoop.hive.ql.optimizer.ReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.util.*;

public class BeamCompiler extends TaskCompiler {

  // not required for POC
  @Override
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx, GlobalLimitCtx globalLimitCtx) throws SemanticException {
  }

  // not required for POC
  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx, Context ctx) throws SemanticException {
  }

  // this seems to be really used only to enable bucketizedhiveinputformat on tasks, not required for POC
  @Override
  protected void setInputFormat(Task<? extends Serializable> rootTask) {
  }

  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx, List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
    LOG.info("Processing operator tree in Beam compiler");
    LOG.info("rootTasks={}", BeamUtils.printTasks(rootTasks));
    LOG.info("pCtx={}", pCtx);
    LOG.info("pCtx.getTopOps={}", Operator.toString(pCtx.getTopOps().values()));
    LOG.info("mvTask={}", mvTask);
    LOG.info("inputs={}", inputs);
    LOG.info("outputs={}", outputs);

    // (TezTask) TaskFactory.get(new TezWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID), conf), conf);
    Task<?> task = new BeamTask(new BeamWork());
    rootTasks.add(task);
  }
}
