package com.pifi.core;

import java.util.Collections;
import org.junit.Assert;

public final class ProcessorTest {

  public final void createEmptyProcessor() {
    Processor pp = new Processor("p1") {

      @Override
      public void onTrigger(ProcessSession session) throws Exception {}
    };

    Assert.assertEquals("p1", pp.getIdentifier());
    Assert.assertEquals(Collections.EMPTY_LIST, pp.getRelationships());
  }


}
