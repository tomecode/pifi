package com.pifi.core;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowManager {

  private static final Logger logger = LoggerFactory.getLogger(FlowManager.class);

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock readLock = rwLock.readLock();
  private final Lock writeLock = rwLock.writeLock();

  private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Connectable> allProcessors = new ConcurrentHashMap<>();


  private final AtomicInteger maxTimerDrivenThreads;
  private final FlowEngine timerDrivenFlowEngine;

  private final ProcessScheduler processScheduler;

  public FlowManager() {
    maxTimerDrivenThreads = new AtomicInteger(2);
    timerDrivenFlowEngine = new FlowEngine(maxTimerDrivenThreads.get(), "Timer-Driven Process");
    final TimerDrivenSchedulingAgent timerDrivenAgent = new TimerDrivenSchedulingAgent(timerDrivenFlowEngine);
    processScheduler = new ProcessScheduler(timerDrivenFlowEngine, timerDrivenAgent);

    logger.info("Init flow mananger, with Timer-Driven threads={}", maxTimerDrivenThreads);
  }

  public final Connectable addProcessor(Processor processor) {
    writeLock.lock();
    try {

      if (allProcessors.keySet().stream().filter(e -> e.equalsIgnoreCase(processor.getIdentifier())).count() > 0) {
        throw new RuntimeException("Processor with id=" + processor.getIdentifier() + " already exists");
      }
      final Connectable procNode = new Connectable(processor, processScheduler);
      allProcessors.put(processor.getIdentifier(), procNode);
      return procNode;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * create connection between two processors
   * 
   * @param source processor
   * @param destination processor
   * @param rel relationship name
   * @return
   */
  public final Connection addConnection(final Connectable source, final Connectable destination, Relationship rel) {
    writeLock.lock();
    try {
      List<Relationship> relationships = Collections.singletonList(rel);
      Connection connection = new Connection(source, destination, relationships);

      source.addConnection(connection);
      if (source != destination) { // don't call addConnection twice if it's a self-looping connection.
        destination.addConnection(connection);
      }
      this.allConnections.put(connection.getIdentifier(), connection);
      return connection;
    } finally {
      writeLock.unlock();
    }
  }

  public final void startProcessors() {
    try {
      readLock.lock();
      for (Entry<String, Connectable> p : this.allProcessors.entrySet()) {
        processScheduler.startProcessor(p.getValue());
      }
    } finally {
      readLock.unlock();
    }
  }

  public void stopProcessors() {
    readLock.lock();
    try {
      for (Entry<String, Connectable> p : this.allProcessors.entrySet()) {
        processScheduler.stopProcessor(p.getValue());
      }
    } finally {
      readLock.unlock();
    }
  }

}
