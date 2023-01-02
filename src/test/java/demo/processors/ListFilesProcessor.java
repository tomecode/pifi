package demo.processors;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;
import com.pifi.core.Relationship;

/**
 * sample processor that read files form FS and send them to the next queue
 *
 */
public class ListFilesProcessor extends Processor {

  private static final Logger log = LoggerFactory.getLogger(ListFilesProcessor.class);


  private final Path rootDir;
  private final String fileSufix;


  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
  public static final Relationship REL_ERROR = new Relationship.Builder().name("error").build();



  public ListFilesProcessor(String name, Path rootDir, String fileSufix) {
    super(name);
    this.rootDir = rootDir;
    this.fileSufix = fileSufix;
  }

  @Override
  public void onTrigger(ProcessSession session) throws Exception {
    List<Path> result = new ArrayList<>();
    if (Files.isDirectory(rootDir, LinkOption.NOFOLLOW_LINKS)) {
      try (Stream<Path> paths = Files.walk(rootDir)) {
        List<Path> r = paths.filter(p -> Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) &&
            p.getFileName().toString().endsWith(fileSufix))
            .collect(Collectors.toList());

        result.addAll(r);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("processor={} rootDir={} listFiles={}", getIdentifier(), rootDir, result.size());
    }

    for (Path file : result) {
      Path newPath = Files.move(file, Paths.get(file.getParent().toString(), file.getFileName().toString() + "_1"), StandardCopyOption.ATOMIC_MOVE);

      // create flow flow as in nifi and transfer it to the next processor
      FlowFile ff = session.create();
      ff.getAttributes().put("file.original.path", file.toString());
      ff.getAttributes().put("file.read.path", newPath.toString());
      ff.getAttributes().put("time", String.valueOf(System.currentTimeMillis()));

      log.info("processor={} transfer (send) flowFile.Id={}", getIdentifier(), ff.getId());


      // sends a message to the next activity, via relationship 'success'
      session.transfer(ff, REL_SUCCESS);
    }
  }

  /**
   * this is poller
   */
  @Override
  public boolean isTriggerWhenEmpty() {
    return true;
  }

}
