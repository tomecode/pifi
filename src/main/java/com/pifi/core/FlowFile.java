package com.pifi.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.CompareToBuilder;

/**
 *
 *
 */
public final class FlowFile implements Comparable<FlowFile> {

  private long id;
  private long entryDate;
  private long lineageStartDate;
  private long lineageStartIndex;
  private long size;
  private long penaltyExpirationMs;
  private Map<String, String> attributes;
  private long claimOffset;
  private long lastQueueDate;
  private long queueDateIndex;


  public FlowFile(String uuid) {
    this.entryDate = System.currentTimeMillis();
    this.attributes = new HashMap<String, String>();
  }

  public long getId() {
    return id;
  }

  public long getEntryDate() {
    return entryDate;
  }

  public long getLineageStartDate() {
    return lineageStartDate;
  }

  public Long getLastQueueDate() {
    return lastQueueDate;
  }

  public boolean isPenalized() {
    return penaltyExpirationMs > 0 ? penaltyExpirationMs > System.currentTimeMillis() : false;
  }

  public String getAttribute(final String key) {
    return attributes.get(key);
  }

  public long getSize() {
    return size;
  }

  public Map<String, String> getAttributes() {
    return this.attributes;
  }

  public long getContentClaimOffset() {
    return this.claimOffset;
  }


  public long getLineageStartIndex() {
    return lineageStartIndex;
  }

  public long getQueueDateIndex() {
    return queueDateIndex;
  }


  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FlowFile)) {
      return false;
    }
    final FlowFile otherRecord = (FlowFile) other;
    return id == otherRecord.getId();
  }

  @Override
  public int compareTo(FlowFile other) {
    return new CompareToBuilder().append(id, other.getId()).toComparison();

  }



}
