package demo.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.ProcessSession;
import com.pifi.core.api.FlowFile;
import com.pifi.core.api.Processor;

public class PrintProcessor extends Processor {

  private static final Logger log = LoggerFactory.getLogger(PrintProcessor.class);


  public PrintProcessor(String name) {
    super(name);
  }

  @Override
  public void onTrigger(ProcessSession session) throws Exception {
    FlowFile ff = session.get();
    if (ff != null) {
      log.info("processor={} received flowFile.Id={} file.content={}", getIdentifier(), ff.getId(), ff.getAttribute("content"));
    }
  }

}
