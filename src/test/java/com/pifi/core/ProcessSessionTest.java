package com.pifi.core;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import com.pifi.core.api.FlowFile;

public final class ProcessSessionTest {

  @Test
  public final void createEmptySession() {
    ProcessSession mockSession = Mockito.mock(ProcessSession.class);
    Assert.assertNull(mockSession.get());
  }

  @Test
  public final void getFlowFileFromDummySession() {
    ProcessSession mockSession = Mockito.mock(ProcessSession.class);
    Assert.assertNull(mockSession.get());
  }

  @Test
  public final void createFlowFileFromSession() {
    RepositoryContext repositoryContext = Mockito.mock(RepositoryContext.class);
    ProcessSession mockSession = new ProcessSession(repositoryContext);
    FlowFile fa = mockSession.create();
    Assert.assertNotNull(fa.getId());
    FlowFile fb = mockSession.create();
    Assert.assertNotNull(fb.getId());
    Assert.assertNotEquals(fa.getId(), fb.getId());
    Assert.assertFalse(fa.equals(fb));
  }

}
