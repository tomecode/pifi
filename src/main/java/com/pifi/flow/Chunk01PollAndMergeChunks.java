package com.pifi.flow;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.honeywell.ams.bbic.GatewayProperties;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectory;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectoryService;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessContext;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;

public final class Chunk01PollAndMergeChunks extends Processor {

  private static final Logger log = LoggerFactory.getLogger(Chunk01PollAndMergeChunks.class);


  private Path rootChunksDir;
  private Path rootTransferDir;
  private int retentionTime;
  private ChronoUnit retentionTimeUnit;

  private final ContainerDirectoryService containerDirectoryService;

  public Chunk01PollAndMergeChunks(String name, GatewayProperties settings, ContainerDirectoryService cds) throws Exception {
    super(name);
    this.rootChunksDir = settings.getTransferFolderChunksPath();
    this.rootTransferDir = settings.getTransferFolderTransferPath();
    this.retentionTime = settings.getTransferChunksRetentionTime();
    this.retentionTimeUnit = ChronoUnit.valueOf(settings.getTransferChunksRetentionTimeUnit());
    this.containerDirectoryService = cds;
    this.startupHouseKeeping(rootChunksDir);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws Exception {
    if (!Files.isDirectory(rootChunksDir, LinkOption.NOFOLLOW_LINKS)) {
      if (log.isTraceEnabled()) {
        log.trace("Not found transfer directory={}", rootChunksDir);
      }
      return;
    }
    // maximum limit for incomplete upload processing
    Instant retentionLimit = Instant.now().minus(retentionTime, retentionTimeUnit);


    List<Path> containers = getContainers(rootChunksDir);

    //
    // ../containers/...
    //
    for (Path containerDir : containers) {
      if (log.isTraceEnabled()) {
        log.trace("Processing chunks from containerDir={}", containerDir);
      }


      //
      // ../containers/<containerFileDir>/...
      //
      List<Path> containerFilesDir = getContainers(containerDir);

      //
      // ../containers/<containerFileDir>/<chunk files>
      //

      for (Path chunkDir : containerFilesDir) {
        List<Path> chunkFiles = ContainerDirectory.listOfChunks(chunkDir);

        boolean isAfterRetentionLimit = isChunkFolderAfterRetentionLmit(chunkDir, retentionLimit);

        if (chunkFiles.isEmpty()) {
          // there are no chunks
          if (isAfterRetentionLimit) {
            if (log.isInfoEnabled()) {
              log.info("container={} file={} is after retention limi", containerDir, chunkDir);
            }
            // delete old files that have exceeded the processing time limit and have not yet been processed
            containerDirectoryService.deleteFile(Long.parseLong(chunkDir.getFileName().toString()));
          }
          continue;
        }

        //
        // there are some chunks but they are old
        //
        if (isAfterRetentionLimit) {
          if (log.isInfoEnabled()) {
            log.info("container={} file={} is after retention limi", containerDir, chunkDir);
          }
          // delete old files that have exceeded the processing time limit and have not yet been processed
          containerDirectoryService.deleteFile(Long.parseLong(chunkDir.getFileName().toString()));
          continue;
        }



        // if client uploaded the last file
        Path lastChunk = chunkFiles.get(chunkFiles.size() - 1);
        long lastChunkMax = ContainerDirectory.getChunkMax(lastChunk);
        long lastChunkMin = ContainerDirectory.getChunkNum(lastChunk);
        if ((chunkFiles.size() - 1) == (lastChunkMax) && lastChunkMin == lastChunkMax) {

          //
          //
          //
          ContainerDirectory fd = containerDirectoryService.updateFileStatus(
              Long.parseLong(chunkDir.getFileName().toString()), ContainerDirectory.STATUS_MERGING);



          Path finalFile = createNewMergeFile(fd);
          doMerge(fd, chunkFiles, finalFile);


          cleanChunks(chunkDir, chunkFiles);


          FlowFile flowFile = session.create();
          flowFile.getAttributes().put("path", finalFile.toAbsolutePath().toString());
          flowFile.getAttributes().put("containerId", String.valueOf(fd.getParentId()));
          flowFile.getAttributes().put("containerFileId", String.valueOf(fd.getId()));
          session.transfer(flowFile);
        }
      }
    }
  }

  private final Path createNewMergeFile(ContainerDirectory fd) throws Exception {
    // target path to destination file
    Path mergeFile =
        rootTransferDir.resolve(String.valueOf(fd.getParentId())).resolve(String.valueOf(fd.getId()))
            .resolve(String.valueOf(fd.getId()) + ContainerDirectory.FINAL_FILE_EXT)
            .toAbsolutePath();


    if (!Files.exists(mergeFile.getParent(), LinkOption.NOFOLLOW_LINKS)) {
      Files.createDirectories(mergeFile.getParent());
    }

    if (Files.exists(mergeFile, LinkOption.NOFOLLOW_LINKS)) {
      Files.delete(mergeFile);
    }

    Files.createFile(mergeFile, GatewayProperties.getCommonPermissionFileAttr());

    return mergeFile;
  }

  /**
   * startup housekeeping -> when the flow starts, then check if all containers exist in the DB, if
   * not then not exists containers all deleted from FS
   * 
   * @param containerDir
   * @throws Exception
   */
  private final void startupHouseKeeping(Path containerDir) throws Exception {
    for (Path containerHome : getContainers(containerDir)) {
      ContainerDirectory cd = containerDirectoryService.findContainer(Long.parseLong(containerHome.getFileName().toString()));
      if (cd == null) {
        // the container will be removed because it doesn't exist in the DB
        log.info("Housekeeping container={}", containerHome);
        containerDirectoryService.deleteContainerDir(containerHome, ContainerDirectoryService.RT_HOUSEKEEPING);
        transferHouseKeeping(containerHome);
      } else {
        // the container for file will be removed because it doesn't exist in the DB
        for (Path fd : getContainers(containerHome)) {

          cd = containerDirectoryService.findFile(Long.parseLong(fd.getFileName().toString()));
          if (cd == null) {
            containerDirectoryService.deleteContainerDir(containerHome, ContainerDirectoryService.RT_HOUSEKEEPING);
            transferHouseKeeping(containerHome);
          }
        }

        // if container is empty then delete
        if (getContainers(containerHome).isEmpty()) {
          containerDirectoryService.deleteContainerDir(containerHome, ContainerDirectoryService.RT_HOUSEKEEPING);
          transferHouseKeeping(containerHome);
        }
      }
    }
  }

  /**
   * housekeeping-> transfers
   * 
   * @param containerDir
   * @throws IOException
   */
  private final void transferHouseKeeping(Path containerDir) throws IOException {
    // delete transfers
    if (Files.exists(containerDir, LinkOption.NOFOLLOW_LINKS)) {
      List<Path> result = new ArrayList<>();
      try (Stream<Path> paths = Files.list(containerDir)) {
        result =
            paths.filter(e -> Files.isRegularFile(e, LinkOption.NOFOLLOW_LINKS) && e.getFileName().toString().endsWith(".upload"))
                .collect(Collectors.toList());
      }
      for (Path uploadFile : result) {
        Files.deleteIfExists(uploadFile);
        if (log.isInfoEnabled()) {
          log.info("{} - unlinked uploaded chunk file={}", uploadFile, ContainerDirectoryService.RT_HOUSEKEEPING);
        }
      }
    }
  }

  private final List<Path> getContainers(Path containerDir) throws Exception {
    List<Path> result = new ArrayList<>();
    if (Files.exists(containerDir, LinkOption.NOFOLLOW_LINKS)) {
      try (Stream<Path> paths = Files.list(containerDir)) {
        result = paths.filter(e -> Files.isDirectory(e, LinkOption.NOFOLLOW_LINKS)).collect(Collectors.toList());
      }
    }

    return result;
  }

  private final boolean isChunkFolderAfterRetentionLmit(Path path, Instant lastDay) {
    try {
      BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
      FileTime a = attr.creationTime();
      return (lastDay.isAfter(a.toInstant()));

    } catch (IOException e) {
      log.error("container={} failed to read time attribute", e);
    }

    return false;
  }

  /**
   * merge chunks into final file
   * 
   * @param cd
   * @param chunkFiles
   * @param destFile
   * @throws Exception
   */
  private final void doMerge(ContainerDirectory cd, List<Path> chunkFiles, Path destFile) throws Exception {
    Instant mInit = Instant.now();

    try {
      try (FileOutputStream fos = new FileOutputStream(destFile.toFile(), true)) {
        int finalPosition = 0;
        try (FileChannel fosChannnel = fos.getChannel()) {
          List<Path> subList = chunkFiles.subList(0, chunkFiles.size() - 1);
          for (Path srcChunk : subList) {
            try (FileChannel rch = FileChannel.open(srcChunk, StandardOpenOption.READ)) {
              long remaining = rch.size();
              long position = 0;
              while (remaining > 0) {
                long transferred = rch.transferTo(position, remaining, fosChannnel);
                remaining -= transferred;
                position += transferred;
                finalPosition += transferred;
                log.info("container={} merge chunk file={} size={} with position={} into path={} position={}", cd.getId(), srcChunk, transferred,
                    position, destFile, finalPosition);
              }
            }
          }
        }
      }
      log.info("container={} all chunks are merged into path{} size={}", cd.getId(), destFile, Files.size(destFile));
    } catch (Exception ex) {
      log.error("container={} failed to merge chunks into path{}", cd.getId(), destFile, ex);
      throw ex;
    } finally {
      Instant mNow = Instant.now();
      long total = Duration.between(mInit, mNow).toMillis();
      log.info("container={} file={} merge activity total time={}", cd.getId(), destFile, total);
    }
  }

  /**
   * clean chunk dir
   * 
   * @param chunkDir
   * @param chunkFiles
   * @throws Exception
   */
  private final void cleanChunks(Path chunkDir, List<Path> chunkFiles) throws Exception {
    for (Path chunkFile : chunkFiles) {
      Files.deleteIfExists(chunkFile);
    }

    Files.deleteIfExists(chunkDir);

    if (getContainers(chunkDir.getParent()).isEmpty()) {
      Files.deleteIfExists(chunkDir.getParent());
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
