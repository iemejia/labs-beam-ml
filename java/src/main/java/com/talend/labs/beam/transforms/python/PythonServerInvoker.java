package com.talend.labs.beam.transforms.python;

import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.environment.ProcessManager.RunningProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

/**
 * Class used to instantiate the python executable. It manages virtualenv setup and server
 * invocation.
 */
class PythonServerInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(PythonServerInvoker.class);

  /**
   * Used to construct a session ID.
   */
  private static final char[] RND = "abcdefghijklmnopqrstvwxyz0123456789".toCharArray();
  private static final int UID_LENGTH = 16;

  /**
   * Override the location where the python whl can be loaded from.
   * TODO: find dynamically not hard coded.
   */
  // private static final String WHEEL = "/home/rskraba/working/github/labs-beam-ml/lucidoitdoit/dist/lucidoitdoit-0.1-py3-none-any.whl";
  private static final String WHEEL = null;

  /**
   * The location where the python whl can be loaded from.
   * TODO: should be dynamically unzipped from the WHEEL.
   */
  private static final String LUCISETUP_SCRIPT =  "/home/rskraba/working/github/labs-beam-ml/lucidoitdoit/bin/lucisetup";

  private static final boolean WAIT_FOR_SOCKET =  false;

  private static PythonServerInvoker instance = null;

  private final String uid;

  // We reuse Beam's ProcessManager because it has the logic to redirect I/O
  // TODO Decide if we create a specialized copy of this to not bring the full Beam
  // core-construction dependency
  private ProcessManager processManager;

  private Integer port;
  private final String processId = "PythonServerInvoker";


  private static boolean isAvailable(int port) {
    try (Socket ignored = new Socket("localhost", port)) {
      return false;
    } catch (IOException ignored) {
      return true;
    }
  }

  /**
   * We don't use new ServerSocket(0) because it binds the port to the Java process so it is unreliable.
   * @return -1 if not port found.
   */
  private static int findFreePort() {
    for(int i = 50000; i < 65535; i++) {
      if (!isAvailable(i)) {
        continue;
      }
      return i;
    }
    return -1;
  }


  private PythonServerInvoker(String uid) {
    this.uid = uid;
    processManager = ProcessManager.create();

    HashMap<String, String> env = new HashMap<>();
    if (WHEEL != null) {
      env.put("LUCIDOITDOIT_WHL", WHEEL);
    }

    try {

      if (WAIT_FOR_SOCKET) {
        RunningProcess runningProcess = processManager.startProcess(
            processId,
            LUCISETUP_SCRIPT,
            Collections.singletonList(this.uid), env);

        // Wait until the socket file appears and then read it
        Path socket = Paths.get("/tmp", uid, "lucidoitdoit.socket");
        int attempts = 10;
        while (attempts-- > 0) {
          if (Files.exists(socket)) {
            try (Scanner scanner = new Scanner(socket)) {
              port = scanner.nextInt();
              break;
            }
          } else {
            Thread.sleep(1000);
          }
        }

        runningProcess.isAliveOrThrow();
      } else {
        // TODO Assign port dynamically
        this.port = findFreePort();
        RunningProcess runningProcess = processManager.startProcess(
            processId,
            "/home/ismael/projects/lucidoitdoit/env/bin/lucidoitdoit",
            Arrays.asList("server", "--host=localhost:" + this.port, "--multi"),
            new HashMap<>());
        runningProcess.isAliveOrThrow();
      }

      LOG.info("Started driver program");



    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static synchronized PythonServerInvoker create(String uid) {
    if (instance == null) {
      instance = new PythonServerInvoker(uid);
    }
    return instance;
  }

  /**
   * @return a session ID for the PythonServerInvoker.
   */
  public static String createUid() {
    StringBuilder uid = new StringBuilder("luci");
    Random rnd = new Random();
    for (int i = 0; i < UID_LENGTH; i++)
      uid.append(RND[rnd.nextInt(RND.length)]);
    return uid.toString();
  }

  public Integer getPort() {
    return port;
  }
}
