package com.pifi.core;

import org.junit.Assert;
import org.junit.Test;

public final class ConnectionTest {

  @Test
  public final void createRelationship() {
    Assert.assertNotNull(new Relationship.Builder().name("").build());
    Assert.assertNotNull(new Relationship.Builder().name(null).build());
    Assert.assertEquals("abc", new Relationship.Builder().name("abc").build().getName());
  }

  public final void autoTerminatedRelationship() {
    Assert.assertTrue(new Relationship.Builder().name("").build().isAutoTerminated());
    Assert.assertTrue(new Relationship.Builder().name("").autoTerminateDefault(true).build().isAutoTerminated());
    Assert.assertFalse(new Relationship.Builder().name("").autoTerminateDefault(false).build().isAutoTerminated());
  }

  public final void createAnonymous() {
    Assert.assertNull(Relationship.ANONYMOUS.getName());
  }

  public final void createSelf() {
    Assert.assertEquals("", Relationship.SELF.getName());
  }


  @Test
  public final void compareRelationships() {
    Relationship ra = new Relationship.Builder().name("").build();
    Relationship rb = new Relationship.Builder().name("").build();
    Assert.assertTrue(ra.equals(rb));

    Relationship rc = new Relationship.Builder().name("x").build();

    Assert.assertFalse(ra.equals(rc));
  }



}
