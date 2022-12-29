package com.pifi.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessContext;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;

public class PrintProcessor extends Processor {

  private static final Logger log = LoggerFactory.getLogger(PrintProcessor.class);


  public PrintProcessor(String name) {
    super(name);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws Exception {
    FlowFile ff = session.get();
    if (ff != null) {
      log.info("processor={} received flowFile.Id={}", getIdentifier(), ff.getId(), ff.getId());

      log.info("processor={} received flowFile.Id={} file.content={}", getIdentifier(), ff.getId(), ff.getAttribute("content"));
    }
  }

}
