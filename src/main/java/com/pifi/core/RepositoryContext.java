package com.pifi.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RepositoryContext {
  private final AtomicLong connectionIndex;
  private final Connectable connectable;

  public RepositoryContext(final Connectable connectable, final AtomicLong connectionIndex) {
    this.connectable = connectable;
    this.connectionIndex = connectionIndex;
  }

  public Connectable getConnectable() {
    return connectable;
  }

  /**
   * 
   * @return
   */
  public Collection<Connection> getPutConnections() {
    Collection<Connection> collection = connectable.getConnections(null);
    if (collection == null) {
      collection = new ArrayList<>();
    }
    return Collections.unmodifiableCollection(collection);
  }

  /**
   * @return an unmodifiable list containing a copy of all incoming connections for the processor from
   *         which FlowFiles are allowed to be pulled
   */
  public List<Connection> getPollableConnections() {
    if (pollFromSelfLoopsOnly()) {
      final List<Connection> selfLoops = new ArrayList<>();
      for (final Connection connection : connectable.getIncomingConnections()) {
        if (connection.getSource() == connection.getDestination()) {
          selfLoops.add(connection);
        }
      }
      return selfLoops;
    } else {
      return connectable.getIncomingConnections();
    }
  }

  /**
   * @return true if we are allowed to take FlowFiles only from self-loops. This is the case when no
   *         Relationships are available except for self-looping Connections
   */
  private boolean pollFromSelfLoopsOnly() {
    if (isTriggerWhenAnyDestinationAvailable()) {
      // we can pull from any incoming connection, as long as at least one downstream
      // connection
      // is available for each relationship.
      // I.e., we can poll only from self if no relationships are available
      return !Connectables.anyRelationshipAvailable(connectable);
    } else {
      for (final Connection connection : connectable.getConnections()) {
        // A downstream connection is full. We are only allowed to pull from self-loops.
        if (connection.getFlowFileQueue().isFull()) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean isTriggerWhenAnyDestinationAvailable() {
    // if (connectable.getConnectableType() != ConnectableType.PROCESSOR) {
    // return false;
    // }

    // final ProcessorNode procNode = (ProcessorNode) connectable;
    // return procNode.isTriggerWhenAnyDestinationAvailable();
    return false;
  }

  public int getNextIncomingConnectionIndex() {
    final int numIncomingConnections = connectable.getIncomingConnections().size();
    return (int) (connectionIndex.getAndIncrement() % Math.max(1, numIncomingConnections));
  }

}
