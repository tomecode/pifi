package com.pifi.core;

import org.junit.Assert;
import org.junit.Test;


public final class FlowFileQueueTest {

  @Test
  public void createFlowFileQueue() {
    FlowFileQueue ffq = new FlowFileQueue();
    Assert.assertNotNull(ffq.getActiveFlowFiles());
    Assert.assertTrue(ffq.isActiveQueueEmpty());
  }

  @Test
  public void putFlowFileToQueue() {
    FlowFileQueue ffq = new FlowFileQueue();
    ffq.put(new FlowFile());
    Assert.assertEquals(1, ffq.getActiveFlowFiles().size());
  }

  @Test
  public void pollFlowFileToQueue() {
    FlowFileQueue ffq = new FlowFileQueue();
    FlowFile fa = new FlowFile();
    ffq.put(fa);
    FlowFile fb = ffq.poll(null);
    Assert.assertTrue(fa.equals(fb));
  }
}
