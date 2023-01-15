package com.pifi.core.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import com.pifi.core.ProcessSession;
import com.pifi.core.Relationship;

/**
 * The basic unit that is executed when queue contains flowfile
 *
 */
public abstract class Processor {

  private String name;
  private List<Relationship> relationships;


  public Processor(String name) {
    this.relationships = new ArrayList<>();
    this.name = name;
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

  /**
   * if <b>true</b>, the processor starts automatically
   * 
   * @return
   */
  public boolean isTriggerWhenEmpty() {
    return false;
  }

  /**
   * here is business logic
   * 
   * @param session
   * @throws Exception
   */
  public abstract void onTrigger(ProcessSession session) throws Exception;



}
