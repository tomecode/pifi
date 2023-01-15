package com.pifi.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.api.FlowFile;

final class FlowFileQueue {

  private static final Logger log = LoggerFactory.getLogger(FlowFileQueue.class);

  private final TimedLock readLock;
  private final TimedLock writeLock;

  private PriorityQueue<FlowFile> activeQueue;

  public FlowFileQueue() {
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = new TimedLock(lock.readLock(), getIdentifier() + " Read Lock", 100);
    writeLock = new TimedLock(lock.writeLock(), getIdentifier() + " Write Lock", 100);
    this.activeQueue = new PriorityQueue<>(20);
  }

  public List<FlowFile> getActiveFlowFiles() {
    readLock.lock();
    try {
      return new ArrayList<>(activeQueue);
    } finally {
      readLock.unlock("getActiveFlowFiles");
    }
  }

  public boolean isActiveQueueEmpty() {
    return activeQueue.size() == 0;
  }

  public String getIdentifier() {
    return "queue";
  }

  public boolean isFull() {
    return false;
  }

  public FlowFile poll(final Set<FlowFile> expiredRecords) {
    FlowFile flowFile;
    writeLock.lock();
    try {
      flowFile = this.activeQueue.poll();
      if (flowFile != null) {
        log.trace("{} poll() returning {}", this, flowFile);
      }

      return flowFile;
    } finally {
      writeLock.unlock("poll(Set)");
    }
  }

  public void put(final FlowFile flowFile) {
    writeLock.lock();
    try {
      activeQueue.add(flowFile);
      log.trace("{} put to {}", flowFile, this);
    } finally {
      writeLock.unlock("put(FlowFileRecord)");
    }
  }


  private static class TimedLock {

    private final Lock lock;

    private final Logger logger;

    private long lockTime = 0L;

    private final Map<String, Long> lockIterations = new HashMap<>();
    private final Map<String, Long> lockNanos = new HashMap<>();

    private final String name;
    private final int iterationFrequency;

    public TimedLock(final Lock lock, final String name, final int iterationFrequency) {
      this.lock = lock;
      this.name = name;
      this.iterationFrequency = iterationFrequency;
      logger = LoggerFactory.getLogger(TimedLock.class.getName() + "." + name);
    }


    public void lock() {
      logger.trace("Obtaining Lock {}", name);
      lock.lock();
      lockTime = System.nanoTime();
      logger.trace("Obtained Lock {}", name);
    }

    /**
     * @param task to release the lock for
     */
    public void unlock(final String task) {
      if (lockTime <= 0L) {
        lock.unlock();
        return;
      }

      logger.trace("Releasing Lock {}", name);
      final long nanosLocked = System.nanoTime() - lockTime;

      Long startIterations = lockIterations.get(task);
      if (startIterations == null) {
        startIterations = 0L;
      }
      final long iterations = startIterations + 1L;
      lockIterations.put(task, iterations);

      Long startNanos = lockNanos.get(task);
      if (startNanos == null) {
        startNanos = 0L;
      }
      final long totalNanos = startNanos + nanosLocked;
      lockNanos.put(task, totalNanos);

      lockTime = -1L;

      lock.unlock();
      logger.trace("Released Lock {}", name);

      if (iterations % iterationFrequency == 0) {
        logger.debug("Lock {} held for {} nanos for task: {}; total lock iterations: {}; total lock nanos: {}",
            name, nanosLocked, task, iterations, totalNanos);
      }
    }
  }
}
