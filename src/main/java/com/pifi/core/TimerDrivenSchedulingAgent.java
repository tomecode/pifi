package com.pifi.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TimerDrivenSchedulingAgent {
  private final long noWorkYieldNanos;

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  private final FlowEngine flowEngine;

  public TimerDrivenSchedulingAgent(final FlowEngine flowEngine) {
    this.flowEngine = flowEngine;
    this.noWorkYieldNanos = 20000l; // millis
  }

  final void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
    final List<ScheduledFuture<?>> futures = new ArrayList<>();
    if (log.isDebugEnabled()) {
      log.debug("schedule: connectable " + connectable.getIdentifier());
    }

    final ConnectableTask connectableTask = new ConnectableTask(connectable, scheduleState);

    for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
      // Determine the task to run and create it.
      final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

      Runnable trigger = createTrigger(connectableTask, scheduleState, futureRef);

      // Schedule the task to run
      final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(trigger, 0L, 2000000, TimeUnit.MICROSECONDS);

      // now that we have the future, set the atomic reference so that if the
      // component is yielded we
      // are able to then cancel this future.
      futureRef.set(future);

      // Keep track of the futures so that we can update the ScheduleState.
      futures.add(future);
    }

    scheduleState.setFutures(futures);
    if (log.isTraceEnabled()) {
      log.trace("Scheduled {} to run with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }
  }

  private Runnable createTrigger(final ConnectableTask connectableTask, final LifecycleState scheduleState,
      final AtomicReference<ScheduledFuture<?>> futureRef) {
    final Connectable connectable = connectableTask.getConnectable();
    final Runnable yieldDetectionRunnable = new Runnable() {
      @Override
      public void run() {
        // Call the task. It will return a boolean indicating whether or not we should
        // yield
        // based on a lack of work for to do for the component.
        final InvocationResult invocationResult = connectableTask.invoke();
        if (invocationResult.isYield()) {
          // log.debug("Yielding {} due to {}", connectable, invocationResult.getYieldExplanation());
        }

        // If the component is yielded, cancel its future and re-submit it to run again
        // after the yield has expired.
        final long newYieldExpiration = connectable.getYieldExpiration();
        final long now = System.currentTimeMillis();
        if (newYieldExpiration > now) {
          final long yieldMillis = newYieldExpiration - now;
          final long scheduleMillis = connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS);
          final ScheduledFuture<?> scheduledFuture = futureRef.get();
          if (scheduledFuture == null) {
            return;
          }

          // If we are able to cancel the future, create a new one and update the
          // ScheduleState so that it has
          // an accurate accounting of which futures are outstanding; we must then also
          // update the futureRef
          // so that we can do this again the next time that the component is yielded.
          if (scheduledFuture.cancel(false)) {
            final long yieldNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(scheduleMillis), TimeUnit.MILLISECONDS.toNanos(yieldMillis));

            synchronized (scheduleState) {
              if (scheduleState.isScheduled()) {
                final long schedulingNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
                final ScheduledFuture<?> newFuture = flowEngine.scheduleWithFixedDelay(this, yieldNanos, schedulingNanos, TimeUnit.NANOSECONDS);

                scheduleState.replaceFuture(scheduledFuture, newFuture);
                futureRef.set(newFuture);
              }
            }
          }
        } else if (noWorkYieldNanos > 0L && invocationResult.isYield()) {
          // Component itself didn't yield but there was no work to do, so the framework
          // will choose
          // to yield the component automatically for a short period of time.
          final ScheduledFuture<?> scheduledFuture = futureRef.get();
          if (scheduledFuture == null) {
            return;
          }

          // If we are able to cancel the future, create a new one and update the
          // ScheduleState so that it has
          // an accurate accounting of which futures are outstanding; we must then also
          // update the futureRef
          // so that we can do this again the next time that the component is yielded.
          if (scheduledFuture.cancel(false)) {
            synchronized (scheduleState) {
              // if (scheduleState.isScheduled()) {
              if (log.isTraceEnabled()) {
                log.trace("connetable={} is  SHEDULLED", connectable);
              }
              final ScheduledFuture<?> newFuture =
                  flowEngine.scheduleWithFixedDelay(this, noWorkYieldNanos, connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS),
                      TimeUnit.NANOSECONDS);

              scheduleState.replaceFuture(scheduledFuture, newFuture);
              futureRef.set(newFuture);
              if (log.isTraceEnabled()) {
                log.trace("connetable={} is not SHEDULLED", connectable);
              }
            }
          }
        }
      }
    };

    return yieldDetectionRunnable;
  }

  public void shutdown() {
    this.flowEngine.shutdown();
  }
}
