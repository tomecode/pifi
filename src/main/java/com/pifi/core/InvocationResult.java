package com.pifi.core;

public interface InvocationResult {
  boolean isYield();

  String getYieldExplanation();

  public static InvocationResult DO_NOT_YIELD = new InvocationResult() {
    @Override
    public boolean isYield() {
      return false;
    }

    @Override
    public String getYieldExplanation() {
      return null;
    }
  };

  public static InvocationResult yield(final String explanation) {
    return new InvocationResult() {
      @Override
      public boolean isYield() {
        return true;
      }

      @Override
      public String getYieldExplanation() {
        return explanation;
      }
    };
  }
}
