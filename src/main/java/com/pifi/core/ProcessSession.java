package com.pifi.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessSession {
  private static final Logger log = LoggerFactory.getLogger(ProcessSession.class);

  private final RepositoryContext context;

  private final Map<Long, RepositoryRecord> records = new ConcurrentHashMap<>();


  public ProcessSession(RepositoryContext context) {
    this.context = context;
  }

  public FlowFile get() {
    if (log.isDebugEnabled()) {
      log.debug("Getting flow file from {}", this);
    }
    final List<Connection> connections = context.getPollableConnections();
    final int numConnections = connections.size();
    for (int numAttempts = 0; numAttempts < numConnections; numAttempts++) {
      final Connection conn = connections.get(context.getNextIncomingConnectionIndex() % numConnections);
      final Set<FlowFile> expired = new HashSet<>();
      final FlowFile flowFile = conn.poll(expired);

      if (flowFile != null) {
        return flowFile;
      }
    }

    return null;
  }

  public FlowFile create() {
    final Map<String, String> attrs = new HashMap<>();
    final String uuid = UUID.randomUUID().toString();
    attrs.put("UUID", uuid);
    final FlowFile fFile = new FlowFile(uuid);

    final RepositoryRecord record = new RepositoryRecord(null);
    record.setWorking(fFile, attrs, false);
    records.put(fFile.getId(), record);

    return fFile;
  }

  //
  @SuppressWarnings("unlikely-arg-type")
  public void transfer(FlowFile flowFile) {
    RepositoryRecord srr = getRecord(flowFile);

    Collection<Connection> numDestinations = context.getPutConnections();
    for (Connection numDestination : numDestinations) {

      FlowFile recordFile = srr.getCurrent();
      numDestination.enqueue(recordFile);
    }
    this.records.remove(srr);

  }

  private RepositoryRecord getRecord(final FlowFile flowFile) {
    return records.get(flowFile.getId());
  }

}
