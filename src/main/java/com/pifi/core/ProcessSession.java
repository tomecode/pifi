package com.pifi.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    final FlowFile fFile = new FlowFile();
    final RepositoryRecord record = new RepositoryRecord(null);
    record.setWorking(fFile, fFile.getAttributes(), false);
    records.put(fFile.getId(), record);
    return fFile;
  }

  public void transfer(FlowFile flowFile) {
    transfer(flowFile, null);
  }

  @SuppressWarnings("unlikely-arg-type")
  public void transfer(FlowFile flowFile, Relationship rel) {
    RepositoryRecord srr = getRecord(flowFile);
    if (srr != null) {
      Collection<Connection> numDestinations = context.getPutConnections(rel);
      for (Connection numDestination : numDestinations) {

        FlowFile recordFile = srr.getCurrent();
        recordFile.getAttributes().put("identifier", numDestination.getIdentifier());
        recordFile.getAttributes().put("relationships", Arrays.toString(numDestination.getRelationships().toArray()));
        numDestination.enqueue(recordFile);
      }
      this.records.remove(srr);
    }
  }


  private RepositoryRecord getRecord(final FlowFile flowFile) {
    return records.get(flowFile.getId());
  }



}
