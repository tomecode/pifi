package com.pifi.core;

import java.util.Collection;
import java.util.List;

public class Connectables {

  public static boolean flowFilesQueued(final Connectable connectable) {
    for (final Connection conn : connectable.getIncomingConnections()) {
      if (!conn.getFlowFileQueue().isActiveQueueEmpty()) {
        return true;
      }

    }

    return false;
  }

  public static boolean flowFilesQueuedInput(final Connectable connectable) {
    for (final Connection conn : connectable.getIncomingConnections()) {
      if (!conn.getFlowFileQueue().isActiveQueueEmpty()) {
        return true;
      }

    }

    return false;
  }


  public static boolean anyRelationshipAvailable(final Connectable connectable) {
    for (final Relationship relationship : connectable.getRelationships()) {
      final Collection<Connection> connections = connectable.getConnections(relationship);

      boolean available = true;
      for (final Connection connection : connections) {
        if (connection.getFlowFileQueue().isFull()) {
          available = false;
          break;
        }
      }

      if (available) {
        return true;
      }
    }

    return false;
  }

  public static boolean hasNonLoopConnection(final Connectable connectable) {
    final List<Connection> connections = connectable.getIncomingConnections();
    for (final Connection connection : connections) {
      if (!connection.getSource().equals(connectable)) {
        return true;
      }
    }

    return false;
  }
}
