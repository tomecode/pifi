package com.pifi.processors;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessContext;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;

public class ReadFileProcessor extends Processor {

  private static final Logger log = LoggerFactory.getLogger(ReadFileProcessor.class);

  public ReadFileProcessor(String name) {
    super(name);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws Exception {
    FlowFile ff = session.get();
    if (ff != null) {
      log.info("processor={} received flowFile.Id={}", getIdentifier(), ff.getId());

      String content = new String(Files.readAllBytes(Paths.get(ff.getAttribute("file.read.path"))));
      FlowFile newff = session.create();
      newff.getAttributes().putAll(ff.getAttributes());
      newff.getAttributes().put("content", content);
      session.transfer(newff);
    }
  }

}
