package com.pifi.core;

/**
 * An immutable object for holding information about a type of relationship.
 *
 */
public final class Relationship implements Comparable<Relationship> {

  public static final Relationship SELF = new Relationship.Builder().build();
  public static final Relationship ANONYMOUS = new Relationship.Builder().name("").build();

  /**
   * The proper name of the relationship. Determines the relationship 'identity'
   */
  private final String name;


  /**
   * The hash code, which is computed in the constructor because it is hashed very frequently and the
   * hash code is constant
   */
  private final int hashCode;

  /**
   * The flag which tells the controller to auto terminate this relationship, so that the processor
   * can be run even if it does not have connections from this relationship
   */
  private final boolean isAutoTerminate;

  protected Relationship(final Builder builder) {
    this.name = builder.name == null ? null : builder.name.intern();
    this.isAutoTerminate = builder.autoTerminate;
    this.hashCode = 301 + ((name == null) ? 0 : this.name.hashCode()); // compute only once, since it gets called a
                                                                       // bunch and will never change
  }

  @Override
  public int compareTo(final Relationship o) {
    if (o == null) {
      return -1;
    }
    final String thisName = getName();
    final String thatName = o.getName();
    if (thisName == null && thatName == null) {
      return 0;
    }
    if (thisName == null) {
      return 1;
    }
    if (thatName == null) {
      return -1;
    }

    return thisName.compareTo(thatName);
  }

  public static final class Builder {

    private String name = "";
    private boolean autoTerminate = false;

    public Builder name(final String name) {
      if (null != name) {
        this.name = name;
      }
      return this;
    }

    public Builder autoTerminateDefault(boolean autoTerminate) {
      this.autoTerminate = autoTerminate;
      return this;
    }

    public Relationship build() {
      return new Relationship(this);
    }
  }

  public String getName() {
    return this.name;
  }


  public boolean isAutoTerminated() {
    return this.isAutoTerminate;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof Relationship)) {
      return false;
    }
    if (this == other) {
      return true;
    }
    Relationship desc = (Relationship) other;
    return this.name.equals(desc.name);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return this.name;
  }
}
