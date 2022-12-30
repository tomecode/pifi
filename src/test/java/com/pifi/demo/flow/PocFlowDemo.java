package com.pifi.demo.flow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Timer;
import java.util.TimerTask;
import com.pifi.core.Connectable;
import com.pifi.core.FlowController;
import com.pifi.core.FlowManager;
import com.pifi.demo.processors.ListFilesProcessor;
import com.pifi.demo.processors.PrintProcessor;
import com.pifi.demo.processors.ReadFileProcessor;

/**
 * simple demo where are 3 processors (3 activities), each of them connected via relationship
 * 
 *
 */
public final class PocFlowDemo {


  public static void main(String[] args) throws Exception {

    // engine
    FlowController flowController = new FlowController();
    FlowManager flowManager = flowController.getFlowManager();

    // test directory
    Path testDir = Paths.get("target").toAbsolutePath();
    if (!Files.exists(testDir)) {
      Files.createDirectories(testDir);
    }


    // step1 -> reading files from the folder
    Connectable activity1 = flowManager.addProcessor(new ListFilesProcessor("1-list-files", testDir, ".txt"));
    // step2 -> read file content (from previous activity)
    Connectable activit2 = flowManager.addProcessor(new ReadFileProcessor("2-read-content"));
    // step3-> print file content
    Connectable activit3 = flowManager.addProcessor(new PrintProcessor("3-print-content"));

    //
    // simple flow, activity1->connected to->activity2 ->connected to->activity3
    //
    flowManager.addConnection(activity1, activit2, ListFilesProcessor.REL_SUCCESS);
    flowManager.addConnection(activit2, activit3, ListFilesProcessor.REL_SUCCESS);

    
    
    // start processors in flow
    flowManager.startProcessors();


    // simple thread that creates new file
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      public void run() {


        try {
          String baseName = "test_" + System.currentTimeMillis() + ".txt";
          Path testFile = testDir.resolve(baseName + "_c");
          Files.write(testFile, "Hello Nifi in PIFI :)".getBytes(StandardCharsets.UTF_8));
          Files.move(testFile, testDir.resolve(baseName), StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e) {
          e.printStackTrace();
        }

      }
    }, 0, 60 * (1000 * 1));


  }
}
