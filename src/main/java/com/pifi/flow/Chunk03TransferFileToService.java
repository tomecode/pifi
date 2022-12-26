package com.pifi.flow;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.honeywell.ams.bbic.GatewayProperties;
import com.honeywell.ams.bbic.common.service_connector.ServiceConnector;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectory;
import com.honeywell.ams.bbic.dag.upload.files.ContainerDirectoryService;
import com.honeywell.ams.bbic.inspections.attachments.python_model.FileAttachment;
import com.honeywell.ams.bbic.inspections.attachments.python_model.FileType;
import com.pifi.core.FlowFile;
import com.pifi.core.ProcessContext;
import com.pifi.core.ProcessSession;
import com.pifi.core.Processor;

public final class Chunk03TransferFileToService extends Processor {
  private static final Logger log = LoggerFactory.getLogger(Chunk03TransferFileToService.class);

  private static final String ATTACH_FILE_URL = "/inspections/{inspectionId}/file/{filetype}?specific={specific}&user={userId}";

  private final ContainerDirectoryService containerDirectoryService;
  private ServiceConnector inspectionConnector;
  private GatewayProperties settings;

  public Chunk03TransferFileToService(String name, GatewayProperties settings,
      ContainerDirectoryService containerDirectoryService,
      ServiceConnector inspectionConnector) {
    super(name);
    this.settings = settings;
    this.containerDirectoryService = containerDirectoryService;
    this.inspectionConnector = inspectionConnector;
  }

  @Override
  public final void onTrigger(ProcessContext context, ProcessSession session) throws Exception {
    FlowFile ffFromQueue = session.get();
    if (ffFromQueue != null) {
      Path requestFile = Paths.get(ffFromQueue.getAttributes().get("path"));
      long containerId = Long.parseLong(ffFromQueue.getAttributes().get("containerId"));
      long containerFileId = Long.parseLong(ffFromQueue.getAttributes().get("containerFileId"));

      ContainerDirectory fd = containerDirectoryService.updateFileStatus(containerFileId, ContainerDirectory.STATUS_TRANSFERING);
      if (fd == null) {
        log.error("Failed to transfer uploaded file, because container={} file={} not found", containerFileId, requestFile);
        return;
      }


      Path destFile = settings.getTransferFolderPath().resolve(requestFile.getFileName());
      try {

        if (!Files.exists(destFile.getParent(), LinkOption.NOFOLLOW_LINKS)) {
          Files.createDirectories(destFile.getParent(), GatewayProperties.getCommonPermissionFileAttr());
        }

        Files.move(requestFile, destFile,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        // delete the parent folder
        Files.deleteIfExists(requestFile.getParent());

        log.info("container={} file={} path={} trasfered to {}", containerId, containerFileId, requestFile,
            destFile);

      } catch (Exception ex) {
        log.error("container={} file={} path={} failed to transfer to {} ", containerId, containerFileId,
            requestFile, destFile, ex);
        containerDirectoryService
            .updateFileStatus(containerFileId,
                ContainerDirectory.STATUS_TRANSFERING_FAILED);

        // TODO: redirect flow to queue for error handling
      }

      try {
        FileType fileType = FileType.createFrom(fd.getFileType());
        String specific = fd.getSpecific();

        inspectionConnector.post(ATTACH_FILE_URL,
            FileAttachment.builder()
                .file(destFile.getFileName().toString())
                .name(fd.getFileName())
                .build(),
            fd.getForeignId(),
            fileType.getFormatted(),
            specific,
            fd.getUserId());
        log.info("container={} file={} path={} trasfer to {}", containerId, containerFileId, requestFile,
            destFile);

        // remove from container
        containerDirectoryService.deleteFile(containerFileId);
      } catch (Exception ex) {
        log.error("container={} file={} path={} failed to transfer to {} inspection service", containerId,
            containerFileId, requestFile, destFile, ex);
        containerDirectoryService.updateFileStatus(containerFileId,
            ContainerDirectory.STATUS_TRANSFERING_FAILED);

        // TODO: redirect flow to queue for error handling
      }
    }

  }

}
