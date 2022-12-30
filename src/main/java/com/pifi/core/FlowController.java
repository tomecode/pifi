package com.pifi.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowController {

  private static final Logger logger = LoggerFactory.getLogger(FlowController.class);

  private final FlowManager flowManager;
  private final AtomicInteger maxTimerDrivenThreads;
  private final AtomicReference<FlowEngine> timerDrivenEngineRef;

  private final ProcessScheduler processScheduler;

  public FlowController() {
    this.flowManager = new FlowManager(this);
    maxTimerDrivenThreads = new AtomicInteger(2);
    timerDrivenEngineRef = new AtomicReference<>(new FlowEngine(maxTimerDrivenThreads.get(), "Timer-Driven Process"));

    final TimerDrivenSchedulingAgent timerDrivenAgent = new TimerDrivenSchedulingAgent(this, timerDrivenEngineRef.get());

    logger.info("Init flow controller, with Timer-Driven threads=" + maxTimerDrivenThreads);
    processScheduler = new ProcessScheduler(timerDrivenEngineRef.get(), timerDrivenAgent);
    flowManager.setProcessScheduler(processScheduler);
  }

  protected ProcessScheduler getProcessScheduler() {
    return this.processScheduler;
  }

  public FlowManager getFlowManager() {
    return flowManager;
  }



}
