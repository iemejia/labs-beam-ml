package com.talend.labs.beam.transforms.python;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.environment.ProcessManager.RunningProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to instantiate the python executable. It manages virtualenv setup and server
 * invocation.
 */
class PythonServerInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(PythonServerInvoker.class);

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

  private PythonServerInvoker() {
    processManager = ProcessManager.create();
    // TODO Assign port dynamically
    this.port = findFreePort();
    try {
      // TODO virtualenv setup
//      processManager.startProcess(
//          processId,
//          "python",
//          Arrays.asList("-m", "http.server", String.valueOf(port)),
//          new HashMap<>());
      RunningProcess runningProcess = processManager.startProcess(
              processId,
              "/home/ismael/projects/lucidoitdoit/env/bin/lucidoitdoit",
              Arrays.asList("server", "--host=localhost:" + this.port, "--multi"),
              new HashMap<>());
      runningProcess.isAliveOrThrow();
      LOG.info("Started driver program");

    } catch (IOException e) {
      new RuntimeException(e);
    }
  }

  private static PythonServerInvoker instance = null;

  public static synchronized PythonServerInvoker create() {
    if (instance == null) {
      instance = new PythonServerInvoker();
    }
    return instance;
  }

  public Integer getPort() {
    return port;
  }
}
