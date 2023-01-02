package com.pifi.core;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProcessScheduler {

  private static final Logger log = LoggerFactory.getLogger(ProcessScheduler.class);


  private final long administrativeYieldMillis;
  private final long processorStartTimeoutMillis;

  private final ScheduledExecutorService frameworkTaskExecutor;

  private final ConcurrentMap<Object, LifecycleState> lifecycleStates = new ConcurrentHashMap<>();

  private TimerDrivenSchedulingAgent schedulingAgent;

  // thread pool for starting/stopping components
  private final ScheduledExecutorService componentLifeCycleThreadPool;
  private final ScheduledExecutorService componentMonitoringThreadPool = new FlowEngine(2, "Monitor Processor Lifecycle", true);

  public ProcessScheduler(FlowEngine componentLifecycleThreadPool, TimerDrivenSchedulingAgent timerDrivenSchedulingAgent) {
    this.componentLifeCycleThreadPool = componentLifecycleThreadPool;
    this.schedulingAgent = timerDrivenSchedulingAgent;

    administrativeYieldMillis = 30000l; // MILLISECONDS
    processorStartTimeoutMillis = 12000;//
    frameworkTaskExecutor = new FlowEngine(1, "Framework Task Thread");
  }


  public void shutdown() {
    this.schedulingAgent.shutdown();
    frameworkTaskExecutor.shutdown();
    componentLifeCycleThreadPool.shutdown();
  }

  /**
   * start given processor
   * 
   * @param procNode
   * @return
   */
  public synchronized CompletableFuture<Void> startProcessor(final Connectable procNode) {
    final LifecycleState lifecycleState = getLifecycleState(procNode, true);

    final CompletableFuture<Void> future = new CompletableFuture<>();
    final SchedulingAgentCallback callback = new SchedulingAgentCallback() {
      @Override
      public void trigger() {
        lifecycleState.clearTerminationFlag();
        schedulingAgent.doSchedule(procNode, lifecycleState);
        future.complete(null);
      }

      @Override
      public Future<?> scheduleTask(final Callable<?> task) {
        lifecycleState.incrementActiveThreadCount(null);
        return componentLifeCycleThreadPool.submit(task);
      }

      @Override
      public void onTaskComplete() {
        lifecycleState.decrementActiveThreadCount();
      }
    };

    log.info("Starting {}", procNode);
    procNode.start(componentMonitoringThreadPool, administrativeYieldMillis, processorStartTimeoutMillis, callback);
    return future;
  }

  private final LifecycleState getLifecycleState(final Object schedulable, final boolean replaceTerminatedState) {
    LifecycleState lifecycleState;
    while (true) {
      lifecycleState = this.lifecycleStates.get(schedulable);

      if (lifecycleState == null) {
        lifecycleState = new LifecycleState();
        final LifecycleState existing = this.lifecycleStates.putIfAbsent(schedulable, lifecycleState);

        if (existing == null) {
          break;
        } else {
          continue;
        }
      } else if (replaceTerminatedState && lifecycleState.isTerminated()) {
        final LifecycleState newLifecycleState = new LifecycleState();
        final boolean replaced = this.lifecycleStates.replace(schedulable, lifecycleState, newLifecycleState);

        if (replaced) {
          lifecycleState = newLifecycleState;
          break;
        } else {
          continue;
        }
      } else {
        break;
      }
    }

    return lifecycleState;
  }

  /**
   * 
   * @param processor
   * @return
   */
  public Future<Void> stopProcessor(Connectable processor) {

    return null;
  }


  public interface SchedulingAgentCallback {
    void onTaskComplete();

    Future<?> scheduleTask(Callable<?> task);

    void trigger();
  }

}
