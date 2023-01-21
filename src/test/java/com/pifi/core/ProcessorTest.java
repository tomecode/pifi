package com.pifi.core;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import com.pifi.core.api.Processor;

public final class ProcessorTest {

  @Test
  public final void createEmptyProcessor() {
    Processor pp = new Processor("p1") {

      @Override
      public void onTrigger(ProcessSession session) throws Exception {}
    };

    Assert.assertEquals("p1", pp.getIdentifier());
    Assert.assertEquals(Collections.EMPTY_LIST, pp.getRelationships());
  }

  @Test
  public final void lifecylceProcessor() throws Exception {
    FlowManager fw = new FlowManager();
    Connectable pInstance = fw.addProcessor(new Processor("p1") {

      @Override
      public void onTrigger(ProcessSession session) throws Exception {

      }
    });

    Assert.assertEquals(ScheduledState.STOPPED, pInstance.getScheduledState());
    Assert.assertEquals(ScheduledState.STOPPED, pInstance.getDesiredState());

    // start processor
    fw.startProcessors();

    Assert.assertEquals(ScheduledState.RUNNING, pInstance.getScheduledState());
    // start processor
    fw.stopProcessors();
    Thread.sleep(5000);
    Assert.assertEquals(ScheduledState.STOPPED, pInstance.getScheduledState());


  }



}
