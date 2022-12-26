package com.pifi.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectableTask {
  private static final Logger log = LoggerFactory.getLogger(ConnectableTask.class);

  private final TimerDrivenSchedulingAgent schedulingAgent;
  private final Connectable connectable;
  private final ProcessContext processContext;
  private final FlowController flowController;
  private final AtomicLong invocations = new AtomicLong(0L);
  private final LifecycleState scheduleState;

  private final RepositoryContext repositoryContext;

  public ConnectableTask(final TimerDrivenSchedulingAgent schedulingAgent, final Connectable connectable,
      final FlowController flowController, final LifecycleState scheduleState) {

    this.schedulingAgent = schedulingAgent;
    this.connectable = connectable;
    this.scheduleState = scheduleState;
    this.flowController = flowController;

    repositoryContext = new RepositoryContext(connectable, invocations);
    processContext = new ProcessContext((Connectable) connectable);
  }

  public Connectable getConnectable() {
    return connectable;
  }

  /**
   * Make sure processor has work to do.
   * 
   * @return
   */
  private boolean isWorkToDo() {
    boolean hasNonLoopConnection = Connectables.hasNonLoopConnection(connectable);

    final boolean isSourceComponent = connectable.isTriggerWhenEmpty()
    // No input connections
        || !connectable.hasIncomingConnection()
        // Every incoming connection loops back to itself, no inputs from other
        // components
        || !hasNonLoopConnection;

    boolean isEmptyQueue = Connectables.flowFilesQueued(connectable);

    return isSourceComponent || isEmptyQueue;
  }

  public InvocationResult invoke() {
    if (scheduleState.isTerminated()) {
      log.debug("Will not trigger {} because task is terminated", connectable);
      return InvocationResult.DO_NOT_YIELD;
    }

    // Make sure processor has work to do.
    if (!isWorkToDo()) {
      if(log.isTraceEnabled()) {
          log.trace("Yielding {} because it has no work to do", connectable);          
      }
      return InvocationResult.yield("No work to do");
    }


    
    //log.debug("Triggering {}", connectable);
    // final long totalInvocationCount = invocations.getAndIncrement();
    // final ProcessSession rawSession;



    final ProcessSession session = new ProcessSession(repositoryContext);
    // WeakHashMapProcessSessionFactory(sessionFactory);
    scheduleState.incrementActiveThreadCount(session);

    final long batchNanos = connectable.getRunDuration(TimeUnit.NANOSECONDS);
    final long startNanos = System.nanoTime();
    // final long finishIfBackpressureEngaged = startNanos + (batchNanos / 25L);
    final long finishNanos = startNanos + batchNanos;
    int invocationCount = 0;

    final String originalThreadName = Thread.currentThread().getName();
    try {
      boolean shouldRun = true;// connectable.getScheduledState() == ScheduledState.RUNNING;
      // || connectable.getScheduledState() == ScheduledState.RUN_ONCE;
      while (shouldRun) {
        invocationCount++;
        connectable.onTrigger(processContext, session);

        final long nanoTime = System.nanoTime();
        if (nanoTime > finishNanos) {
          return InvocationResult.DO_NOT_YIELD;
        }

        if (!isWorkToDo()) {
          // return InvocationResult.DO_NOT_YIELD;
          break;
        }
      }
    } catch (final Throwable e) {
      log.error("Processing halted: uncaught exception in Component [{}]", connectable, e);
    }
    try {

    } finally {
      scheduleState.decrementActiveThreadCount();
      Thread.currentThread().setName(originalThreadName);
    }
    return InvocationResult.DO_NOT_YIELD;
  }

}
