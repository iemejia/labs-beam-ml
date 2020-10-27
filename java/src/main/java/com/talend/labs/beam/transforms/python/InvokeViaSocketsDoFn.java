package com.talend.labs.beam.transforms.python;

import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InvokeViaSocketsDoFn extends DoFn<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(InvokeViaSocketsDoFn.class);

  private final String code;
  private final String requirements;

  private Client client;
  private String codeId;

  InvokeViaSocketsDoFn(String code, String requirements) {
    this.code = code;
    this.requirements = requirements;
  }

  @Setup
  public void setup() throws IOException {
    String host = "localhost";
    // We start the target server that will process the requests
//    PythonServerInvoker pythonServerInvoker = PythonServerInvoker.create();
//    int port = pythonServerInvoker.getPort();
    int port = 50007;
    if (this.client == null) {
      this.client = new Client(host, port);
      this.codeId = client.registerCode(code);
    }
  }

  @StartBundle
  public void startBundle() {}

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<String> outputReceiver) {
    String output = client.execute(codeId, record);
    outputReceiver.output(output);
  }

  @FinishBundle
  public void finishBundle() {}

  @Teardown
  public void teardown() {
    client.close();
  }
}
