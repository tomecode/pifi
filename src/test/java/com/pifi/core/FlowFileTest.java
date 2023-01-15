package com.pifi.core;

import org.junit.Assert;
import org.junit.Test;
import com.pifi.core.api.FlowFile;

public final class FlowFileTest {

  @Test
  public final void createEmptyFlowFile() {
    FlowFile ff = new FlowFile();
    Assert.assertNotNull(ff.getId());
    Assert.assertNotNull(ff.getEntryDate());
    Assert.assertNotNull(ff.getAttributes());
  }
}
