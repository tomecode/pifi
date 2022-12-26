package com.pifi.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class Processor {

  private String name;
  private List<Relationship> relationships;


  public Processor(String name) {
    this.name = name;
    this.relationships = new ArrayList<>();
  }

  public Collection<Relationship> getRelationships() {
    return this.relationships;
  }

  /**
   * 
   * @return the unique identifier that the framework assigned to this component
   */
  public final String getIdentifier() {
    return this.name;
  }

  public boolean isTriggerWhenEmpty() {
    return false;
  }

  /**
   * 
   * @param context
   * @param session
   * @throws Exception
   */
  public abstract void onTrigger(ProcessContext context, ProcessSession session) throws Exception;



}
