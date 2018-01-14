package org.apache.hadoop.hive.ql.parse.beam;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Task;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

class BeamUtils {

  static String printTasks(List<Task<? extends Serializable>> rootTasks) {
    List<String> reprs = new LinkedList<>();
    for (Task task: rootTasks) {
      reprs.add(task.getClass().getName());
    }
    return StringUtils.join(reprs, ',');
  }

}
