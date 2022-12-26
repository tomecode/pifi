package com.pifi.core;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface SchedulingAgentCallback {
  void onTaskComplete();

  Future<?> scheduleTask(Callable<?> task);

  void trigger();
}
