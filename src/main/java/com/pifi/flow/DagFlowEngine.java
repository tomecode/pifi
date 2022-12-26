package com.pifi.flow;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import com.honeywell.ams.bbic.Constants;
import com.honeywell.ams.bbic.GatewayProperties;
import com.honeywell.ams.bbic.common.service_connector.ServiceConnector;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectoryService;
import com.pifi.core.Connectable;
import com.pifi.core.FlowController;
import com.pifi.core.FlowManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DagFlowEngine {

  @Autowired
  private GatewayProperties setttings;

  @Autowired
  private ContainerDirectoryService containerDirectoryService;

  private FlowController flowController = new FlowController();

  @Autowired
  @Qualifier(Constants.PYTHON_INSPECTIONS_SERVICE_CONNECTOR)
  private ServiceConnector inspectionConnector;


  @PostConstruct
  public void init() throws Exception {
    log.info("Dag Engine initialized");


    FlowManager flowManager = flowController.getFlowManager();

    Connectable step1 = flowManager.addProcessor(new Chunk01PollAndMergeChunks("1-chunk-to-file", setttings, containerDirectoryService));
    Connectable step2 = flowManager.addProcessor(new Chunk02AntivirusScan("2-scan-antivirus", setttings, containerDirectoryService));
    Connectable step3 =
        flowManager.addProcessor(new Chunk03TransferFileToService("3-scan-antivirus", setttings, containerDirectoryService, inspectionConnector));

    flowManager.addConnection(step1, step2);
    flowManager.addConnection(step2, step3);

    flowManager.startProcessors();
  }



  @PreDestroy
  public void shutdown() {
    log.info("Dag Engine shutdown");
    if (flowController != null) {

      flowController.getFlowManager().stopProcessors();
      flowController.getProcessScheduler().shutdown();
    }
  }
}
