package com.pifi.core;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FlowManager {

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock readLock = rwLock.readLock();
  private final Lock writeLock = rwLock.writeLock();

  private final ConcurrentMap<String, Connection> allConnections = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Connectable> allProcessors = new ConcurrentHashMap<>();

  private final FlowController flowController;

  private ProcessScheduler processScheduler;

  public FlowManager(FlowController flowController) {
    this.flowController = flowController;
    this.processScheduler = this.flowController.getProcessScheduler();
  }

  public final Connectable addProcessor(Processor processor) {
    writeLock.lock();
    try {

      if (allProcessors.keySet().stream().filter(e -> e.equalsIgnoreCase(processor.getIdentifier())).count() > 0) {
        throw new RuntimeException("Processor with id=" + processor.getIdentifier() + " already exists");
      }
      final Connectable procNode = new Connectable(processor, flowController.getProcessScheduler());
      allProcessors.put(processor.getIdentifier(), procNode);
      return procNode;
    } finally {
      writeLock.unlock();
    }
  }

  public final void setProcessScheduler(ProcessScheduler processScheduler) {
    this.processScheduler = processScheduler;
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


  public final Connection addConnection(final Connectable source, final Connectable destination) {
    writeLock.lock();
    try {
      List<Relationship> relationships = Collections
          .singletonList(new Relationship.Builder().name("success").build());
      Connection connection = new Connection(source, destination, // this.processScheduler,
          relationships);

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


}
