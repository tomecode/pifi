package com.pifi.flow;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.honeywell.ams.bbic.GatewayProperties;
import com.honeywell.ams.bbic.GatewayProperties.FileScan;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectory;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectoryService;
import com.honeywell.ams.bbic.security.scanner.FileContentScanner;
import com.honeywell.ams.bbic.security.scanner.factory.FileContentScannerFactory;
import com.honeywell.ams.bbic.security.validator.FileContentValidator;
import com.honeywell.ams.bbic.security.validator.factory.FileContentValidatorFactory;
import com.honeywell.ams.bbic.security.validator.impl.FileValidatorConstant;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessContext;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;

public class Chunk02AntivirusScan extends Processor {
  private static final Logger log = LoggerFactory.getLogger(Chunk02AntivirusScan.class);

  private final ContainerDirectoryService containerDirectoryService;

  private static final String FILE_CONTENT_VALIDATOR_TYPE = "basic";

  private final long maxFileSize;
  private FileContentScanner scanner;
  private final FileContentValidator validator;
  private final GatewayProperties settings;

  private static final long CLAMAV_SIZE = 4294967295l; // MB 4294.967295
  private final long CLAMAV_SIZE_SPLIT = 1000000000l; // MB 1000

  public Chunk02AntivirusScan(String name, GatewayProperties settings,
      ContainerDirectoryService containerDirectoryService) {
    super(name);

    this.settings = settings;
    FileScan fileScanConfig = this.settings.getFileScan();
    this.containerDirectoryService = containerDirectoryService;
    this.maxFileSize = fileScanConfig.getMaxFileSize();

    if (fileScanConfig.isEnabled()) {
      String scanEngineHostName = fileScanConfig.getEngineHostname();
      int scanEnginePortNumber = fileScanConfig.getEnginePort();
      String scanEngineType = fileScanConfig.getEngineType();
      this.scanner = FileContentScannerFactory.getFileContentScanner(
          scanEngineHostName, scanEnginePortNumber, this.settings.getTransferFolderPath().toString(),
          scanEngineType);
    }

    this.validator = FileContentValidatorFactory.getFileContentValidator(FILE_CONTENT_VALIDATOR_TYPE,
        this.settings.getTransferFolderPath().toString(), maxFileSize);
  }

  public void onTrigger(ProcessContext context, ProcessSession session) throws Exception {

    if (session != null) {
      FlowFile ffFromQueue = session.get();
      if (ffFromQueue != null) {
        Path requestFile = Paths.get(ffFromQueue.getAttributes().get("path"));
        long containerFileId = Long.parseLong(ffFromQueue.getAttributes().get("containerFileId"));

        ContainerDirectory fd = containerDirectoryService.updateFileStatus(containerFileId, ContainerDirectory.STATUS_VALIDATION);
        if (fd == null) {
          log.error("Failed to start uploaded file scan, because container={} file={} not found", containerFileId, requestFile);
          return;
        }

        log.info("container={} fileName={} file={}, validation", fd.getId(), fd.getFileName(), requestFile);

        boolean isOk = containerDirectoryService.checkIntegrity(fd.getHash(), requestFile);
        if (!isOk) {
          log.error(
              "container={} fileName={} file={}, failed to verify integrity of uploaded original checksum (from client) vs. server (calculated) checksum, checksums are not the same, the file will be deleted",
              fd.getId(), fd.getFileName(), requestFile);
          containerDirectoryService.deleteFile(fd.getId());
          return;
        }

        isOk = doValidateBasic(requestFile, fd);
        if (!isOk) {
          log.error("container={} fileName={} file={},the uploaded file is not valid, the file will be deleted", fd.getId(), fd.getFileName(),
              requestFile);
          containerDirectoryService.deleteFile(fd.getId());
          return;

        }

        // Scanning of the file content against present viruses, malware or other
        // malicious content.
        if (scanner != null) {
          if (!(isOk = doScan(requestFile, fd))) {
            log.error("container={} fileName={} file={}, the uploaded file is not valid by antivirus, the file will be deleted", fd.getId(),
                fd.getFileName(), requestFile);
            containerDirectoryService.deleteFile(fd.getId());
            return;
          }
        }
        if (validator != null) {
          if (!(isOk = doValidateFull(requestFile, fd))) {
            log.error("container={} fileName={} file={}, the uploaded file is not valid, the file will be deleted", fd.getId(), fd.getFileName(),
                requestFile);
            containerDirectoryService.deleteFile(fd.getId());
            return;
          }
        }

        log.info("container={} fileName={} file={}, the uploaded file is valid", fd.getId(), fd.getFileName(), requestFile);
        FlowFile ff = session.create();
        ff.getAttributes().putAll(ffFromQueue.getAttributes());
        session.transfer(ff);
      }
    }
  }

  /**
   * basic file validation
   * 
   * @param requestFile
   * @param fd
   * @return
   * @throws IOException
   */
  private final boolean doValidateBasic(Path requestFile, ContainerDirectory fd) throws IOException {
    long fileSize = Files.size(requestFile);
    if (fileSize > this.maxFileSize) {
      log.error("container={} file={} path={} the file is larger (size={}) than the maximum allowed (size={})",
          fd.getParentId(), fd.getId(), requestFile, fileSize, this.maxFileSize);
      // TODO: redirect to error queue
      return false;
    }
    return true;
  }

  /**
   * complex/full file validation
   * 
   * @param requestFile
   * @param fd
   * @return
   * @throws IOException
   */
  private final boolean doValidateFull(Path requestFile, ContainerDirectory fd) throws IOException {
    Instant mInit = Instant.now();
    try {
      containerDirectoryService.updateFileStatus(fd.getId(), ContainerDirectory.STATUS_VALIDATION);
      boolean isOk = validator.validateFile(requestFile.toFile(), fd.getFileName(),
          FileValidatorConstant.INSPECTION_UPLOAD_FILE);
      String status = isOk ? ContainerDirectory.STATUS_VALIDATION_OK : ContainerDirectory.STATUS_VALIDATION_FAILED;
      log.info("container={} file={} path={} the result of the validation is {}", fd.getParentId(), fd.getId(),
          requestFile, status);
      containerDirectoryService.updateFileStatus(fd.getId(), status);
      return isOk;
    } finally {
      Instant mNow = Instant.now();
      long total = Duration.between(mInit, mNow).toMillis();
      log.info("container={} file={} path={} validation total time={}", fd.getParentId(), fd.getId(), total);
    }
  }

  /**
   * antivirus scanning
   * 
   * @param requestFile
   * @param fd
   * @return
   * @throws Exception
   */
  private final boolean doScan(Path requestFile, ContainerDirectory fd) throws Exception {
    Instant mInit = Instant.now();

    long size = Files.size(requestFile);
    List<Path> filesForScan = new ArrayList<>();

    boolean isOk = false;
    if (size > CLAMAV_SIZE) {
      log.info("container={} file={} path={} original file is too big and will split into smaller blocks",
          fd.getParentId(), fd.getId(), requestFile);
      try (FileChannel rch = FileChannel.open(requestFile, StandardOpenOption.READ)) {
        long remaining = rch.size();
        long position = 0;
        while (remaining > 0) {
          Path scanBlock = requestFile.getParent()
              .resolve(requestFile.getFileName() + ".scan" + (filesForScan.size() + 1));
          filesForScan.add(scanBlock);

          log.info("container={} file={} path={} block={}", fd.getParentId(), fd.getId(), requestFile,
              scanBlock);
          if (Files.exists(scanBlock, LinkOption.NOFOLLOW_LINKS)) {
            Files.deleteIfExists(scanBlock);
          }
          try (OutputStream os = Files.newOutputStream(scanBlock, StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE)) {
            try (WritableByteChannel dest = Channels.newChannel(os)) {
              long transferred = rch.transferTo(position, CLAMAV_SIZE_SPLIT, dest);
              remaining -= transferred;
              position += transferred;
            }
          }
        }
      }

    } else {
      log.info("container={} file={} path={} scaning the original file", fd.getParentId(), fd.getId(),
          requestFile);
      filesForScan.add(requestFile);
    }

    try {
      for (Path path : filesForScan) {
        log.info("container={} file={} path={} scanning", fd.getParentId(), fd.getId(), path);
        if (!(isOk = scanner.scan(path.toFile()))) {
          break;
        }
      }

      String status = isOk ? ContainerDirectory.STATUS_SCANNING_OK : ContainerDirectory.STATUS_SCANNING_FAILED;
      containerDirectoryService.updateFileStatus(fd.getId(), status);
      log.info("container={} file={} path={} the result of the antivirus check is {}", fd.getParentId(),
          fd.getId(), requestFile, status);
      return isOk;
    } finally {
      Instant mNow = Instant.now();
      long total = Duration.between(mInit, mNow).toMillis();
      log.info("container={} file={} path={} scanning total time={}", fd.getParentId(), fd.getId(), total);

      if (filesForScan.size() > 1) {
        filesForScan.forEach(blockFile -> {
          try {
            Files.deleteIfExists(blockFile);
          } catch (Exception ex) {
            log.error("container={} file={} failed to delete chunk scan file={}", fd.getParentId(),
                fd.getId(), blockFile, ex);
          }
        });
      }
    }
  }

}
