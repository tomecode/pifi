package com.pifi.core;


public enum ScheduledState {

  STOPPED,
  /**
   * Entity is currently scheduled to run
   */
  RUNNING,

  STARTING,

  STOPPING;
}
