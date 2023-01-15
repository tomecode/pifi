package demo.processors;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.ProcessSession;
import com.pifi.core.Relationship;
import com.pifi.core.api.FlowFile;
import com.pifi.core.api.Processor;

public class ReadFileProcessor extends Processor {

  private static final Logger log = LoggerFactory.getLogger(ReadFileProcessor.class);

  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
  public static final Relationship REL_ERROR = new Relationship.Builder().name("error").build();


  public ReadFileProcessor(String name) {
    super(name);
  }

  @Override
  public void onTrigger(ProcessSession session) throws Exception {
    FlowFile ff = session.get();
    if (ff != null) {
      log.info("processor={} received flowFile.Id={}", getIdentifier(), ff.getId());

      // read the file content
      String content = new String(Files.readAllBytes(Paths.get(ff.getAttribute("file.read.path"))));

      // create new flow file
      FlowFile newff = session.create();
      newff.getAttributes().putAll(ff.getAttributes());
      newff.getAttributes().put("content", content);

      // sends a message to the next activity, via relationship 'success'
      session.transfer(newff, REL_SUCCESS);
    }
  }

}
