package com.pifi.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Deprecated
public class ProcessContext {

  private final Connectable procNode;

  public ProcessContext(final Connectable processorNode) {
    this.procNode = processorNode;
  }

  public int getMaxConcurrentTasks() {
    return procNode.getMaxConcurrentTasks();
  }

  public Set<Relationship> getAvailableRelationships() {
    final Set<Relationship> set = new HashSet<>();
    for (final Relationship relationship : procNode.getRelationships()) {
      final Collection<Connection> connections = procNode.getConnections(relationship);
      if (connections.isEmpty()) {
        set.add(relationship);
      } else {
        boolean available = true;
        for (final Connection connection : connections) {
          if (connection.getFlowFileQueue().isFull()) {
            available = false;
          }
        }

        if (available) {
          set.add(relationship);
        }
      }
    }

    return set;
  }

  public boolean hasIncomingConnection() {
    return procNode.hasIncomingConnection();
  }

  public boolean hasNonLoopConnection() {
    return Connectables.hasNonLoopConnection(procNode);
  }

  public boolean hasConnection(final Relationship relationship) {
    final Set<Connection> connections = procNode.getConnections(relationship);
    return connections != null && !connections.isEmpty();
  }

  public String getName() {
    return procNode.getName();
  }


}
