package com.pifi.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RepositoryRecord {

  private FlowFile workingFlowFileRecord = null;
  private Relationship transferRelationship = null;
  private FlowFileQueue destination = null;
  private final FlowFile originalFlowFileRecord;
  private final FlowFileQueue originalQueue;
  private final Map<String, String> originalAttributes;
  private Map<String, String> updatedAttributes = null;

  /**
   * Creates a new record which has no original claim or flow file - it is entirely new
   *
   * @param originalQueue queue
   */
  public RepositoryRecord(final FlowFileQueue originalQueue) {
    this(originalQueue, null);
  }

  /**
   * Creates a record based on given original items
   *
   * @param originalQueue queue
   * @param originalFlowFileRecord record
   */
  public RepositoryRecord(final FlowFileQueue originalQueue, final FlowFile originalFlowFileRecord) {
    this(originalQueue, originalFlowFileRecord, null);
  }

  public RepositoryRecord(final FlowFileQueue originalQueue, final FlowFile originalFlowFileRecord,
      final String swapLocation) {
    this.originalQueue = originalQueue;
    this.originalFlowFileRecord = originalFlowFileRecord;
    this.originalAttributes = originalFlowFileRecord == null ? Collections.emptyMap() : originalFlowFileRecord.getAttributes();
  }


  public FlowFile getCurrent() {
    return (workingFlowFileRecord == null) ? originalFlowFileRecord : workingFlowFileRecord;
  }

  public void setTransferRelationship(final Relationship relationship) {
    transferRelationship = relationship;
  }



  public void setWorking(final FlowFile flowFile, final Map<String, String> updatedAttribs,
      final boolean contentModified) {
    workingFlowFileRecord = flowFile;


    for (final Map.Entry<String, String> entry : updatedAttribs.entrySet()) {
      final String currentValue = originalAttributes.get(entry.getKey());
      if (currentValue == null || !currentValue.equals(entry.getValue())) {
        initializeUpdatedAttributes().put(entry.getKey(), entry.getValue());
      }
    }
  }

  private Map<String, String> initializeUpdatedAttributes() {
    if (updatedAttributes == null) {
      updatedAttributes = new HashMap<>();
    }

    return updatedAttributes;
  }

  public void setWorking(final FlowFile flowFile, final boolean contentModified) {
    workingFlowFileRecord = flowFile;
  }

  public FlowFileQueue getDestination() {
    return destination;
  }

  public Relationship getTransferRelationship() {
    return transferRelationship;
  }

  public FlowFileQueue getOriginalQueue() {
    return originalQueue;
  }

  public void setDestination(final FlowFileQueue destination) {
    this.destination = destination;
  }

  FlowFile getOriginal() {
    return originalFlowFileRecord;
  }
}
