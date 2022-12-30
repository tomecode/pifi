package com.pifi.core;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.builder.CompareToBuilder;

/**
 *
 *
 */
public final class FlowFile implements Comparable<FlowFile> {

  private long id;
  private Map<String, String> attributes;

  public FlowFile() {
    final UUID uuid = UUID.randomUUID();
    this.id = uuid.getMostSignificantBits() & Long.MAX_VALUE;
    this.attributes = new HashMap<String, String>();
    this.attributes.put("entryDate", String.valueOf(System.currentTimeMillis()));
    this.attributes.put("UUID", uuid.toString());
  }

  public long getId() {
    return id;
  }

  public String getAttribute(final String key) {
    return attributes.get(key);
  }

  public Map<String, String> getAttributes() {
    return this.attributes;
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
