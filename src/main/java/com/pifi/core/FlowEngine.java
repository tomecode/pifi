package com.pifi.core;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;

final class FlowEngine extends ScheduledThreadPoolExecutor {

  private static final org.slf4j.Logger log = LoggerFactory.getLogger(FlowEngine.class);

  /**
   * Creates a new instance of FlowEngine
   *
   * @param corePoolSize the maximum number of threads available to tasks running in the engine.
   * @param threadNamePrefix for naming the thread
   */
  public FlowEngine(int corePoolSize, final String threadNamePrefix) {
    this(corePoolSize, threadNamePrefix, false);
  }

  /**
   * Creates a new instance of FlowEngine
   *
   * @param corePoolSize the maximum number of threads available to tasks running in the engine.
   * @param threadNamePrefix for thread naming
   * @param daemon if true, the thread pool will be populated with daemon threads, otherwise the
   *        threads will not be marked as daemon.
   */
  public FlowEngine(int corePoolSize, final String threadNamePrefix, final boolean daemon) {
    super(corePoolSize);

    final AtomicInteger threadIndex = new AtomicInteger(0);
    final ThreadFactory defaultThreadFactory = getThreadFactory();
    setThreadFactory(new ThreadFactory() {
      @Override
      public Thread newThread(final Runnable r) {
        final Thread t = defaultThreadFactory.newThread(r);
        if (daemon) {
          t.setDaemon(true);
        }
        t.setName(threadNamePrefix + " Thread-" + threadIndex.incrementAndGet());
        return t;
      }
    });
  }

  /**
   * Hook method called by the running thread whenever a runnable task is given to the thread to run.
   *
   * @param thread thread
   * @param runnable runnable
   */
  @Override
  protected void beforeExecute(final Thread thread, final Runnable runnable) {
    super.beforeExecute(thread, runnable);
  }

  @Override
  public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit) {
    return super.schedule(wrap(command), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, final long initialDelay, final long period,
      final TimeUnit unit) {
    return super.scheduleAtFixedRate(wrap(command), initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, final long initialDelay, final long delay,
      final TimeUnit unit) {
    return super.scheduleWithFixedDelay(wrap(command), initialDelay, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit) {
    return super.schedule(wrap(callable), delay, unit);
  }

  private Runnable wrap(final Runnable runnable) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          runnable.run();
        } catch (final Throwable t) {
          log.error("Uncaught Exception in Runnable task", t);
        }
      }
    };
  }

  private <T> Callable<T> wrap(final Callable<T> callable) {
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        try {
          return callable.call();
        } catch (final Throwable t) {
          log.error("Uncaught Exception in Callable task", t);
          throw t;
        }
      }
    };
  }

  /**
   * Hook method called by the thread that executed the given runnable after execution of the runnable
   * completed. Logs the fact of completion and any errors that might have occurred.
   *
   * @param runnable runnable
   * @param throwable throwable
   */
  @Override
  protected void afterExecute(final Runnable runnable, final Throwable throwable) {
    super.afterExecute(runnable, throwable);
    if (runnable instanceof FutureTask<?>) {
      final FutureTask<?> task = (FutureTask<?>) runnable;
      try {
        if (task.isDone()) {
          if (task.isCancelled()) {
            if (log.isTraceEnabled()) {
              log.trace("A flow controller execution task '{}' has been cancelled.", runnable);
            }
          } else {
            task.get(); // to raise any exceptions that might have occurred.
            log.debug("A Flow Controller execution task '{}' has completed.", runnable);
          }
        }
      } catch (final CancellationException ce) {
        if (log.isDebugEnabled()) {
          log.debug("A flow controller execution task '{}' has been cancelled.", runnable);
        }
      } catch (final InterruptedException ie) {
        if (log.isDebugEnabled()) {
          log.debug("A flow controller execution task has been interrupted.", ie);
        }
      } catch (final ExecutionException ee) {
        log.error("A flow controller task execution stopped abnormally", ee);
      }
    } else {
      log.debug("A flow controller execution task '{}' has finished.", runnable);
    }
  }

  /**
   * Hook method called whenever the engine is terminated.
   */
  @Override
  protected void terminated() {
    super.terminated();
  }
}
