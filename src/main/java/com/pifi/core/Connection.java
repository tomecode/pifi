package com.pifi.core;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class Connection {

  private Connectable source;
  private final AtomicReference<Connectable> destination;
  private volatile FlowFileQueue flowFileQueue;
  // private ProcessScheduler scheduler;
  private final AtomicReference<Collection<Relationship>> relationships;

  public Connection(Connectable source, Connectable destination, // ProcessScheduler scheduler,
      List<Relationship> relationships) {
    this.source = source;
    this.destination = new AtomicReference<>(destination);
    flowFileQueue = new FlowFileQueue();
    // this.scheduler = scheduler;
    if (relationships.isEmpty()) {
      relationships.add(Relationship.ANONYMOUS);
    }
    this.relationships = new AtomicReference<>(Collections.unmodifiableCollection(relationships));
  }

  protected FlowFileQueue getFlowFileQueue() {
    return this.flowFileQueue;
  }

  protected Connectable getSource() {
    return this.source;
  }

  protected Connectable getDestination() {
    return destination.get();
  }

  public String getIdentifier() {
    return this.source.getIdentifier() + "->" + this.destination.get().getIdentifier();
  }


  protected FlowFile poll(final Set<FlowFile> expiredRecords) {
    return flowFileQueue.poll(expiredRecords);
  }

  /**
   * Gives this Connection ownership of the given FlowFile and allows the Connection to hold on to the
   * FlowFile but NOT provide the FlowFile to consumers. This allows us to ensure that the Connection
   * is not deleted during the middle of a Session commit.
   *
   * @param flowFile to add
   */
  protected final void enqueue(final FlowFile flowFile) {
    flowFileQueue.put(flowFile);
  }

  protected void setDestination(final Connectable newDestination) {
    final Connectable previousDestination = destination.get();
    if (previousDestination.equals(newDestination)) {
      return;
    }


    try {
      this.destination.set(newDestination);
      getSource().updateConnection(this);

      newDestination.addConnection(this);
    } catch (final RuntimeException e) {
      this.destination.set(previousDestination);
      throw e;
    }
  }

  protected Collection<Relationship> getRelationships() {
    return relationships.get();
  }

}
