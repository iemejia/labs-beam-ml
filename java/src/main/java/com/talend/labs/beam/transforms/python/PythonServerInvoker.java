package com.talend.labs.beam.transforms.python;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
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

  private PythonServerInvoker() {
    processManager = ProcessManager.create();
    // TODO Assign port dynamically
    this.port = 50007;
    try {
      // TODO virtualenv setup
      processManager.startProcess(
              processId,
              "python",
              Arrays.asList("-m", "http.server", String.valueOf(port)),
              new HashMap<>());
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
