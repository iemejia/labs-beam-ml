package com.talend.labs.beam.transforms.python;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InvokeViaSocketsDoFn extends DoFn<String, String> {
  private static final Logger LOG = LoggerFactory.getLogger(InvokeViaSocketsDoFn.class);

  private Socket socket;
  private DataOutputStream dataOutputStream;

  private String code;
  private String requirements;

  InvokeViaSocketsDoFn(String code, String requirements) {
    this.code = code;
    this.requirements = requirements;
  }

  @Setup
  public void setup() throws IOException {
    // We start the target server that will process the requests
    PythonServerInvoker pythonServerInvoker = PythonServerInvoker.create();
    int port = pythonServerInvoker.getPort();
    if (this.socket == null) {
      this.socket = new Socket("localhost", port);
      this.dataOutputStream = new DataOutputStream(this.socket.getOutputStream());
    }
  }

  @StartBundle
  public void startBundle() {}

  @ProcessElement
  public void processElement(@Element String record, OutputReceiver<String> outputReceiver)
      throws IOException {
    String request = "GET /" + System.lineSeparator() + System.lineSeparator();
    dataOutputStream.write(request.getBytes());
    dataOutputStream.flush();

    //    byte[] bytes = record.getBytes(Charset.forName("UTF-8"));
    //    dataOutputStream.write(bytes);
    //    dataOutputStream.flush();

    InputStream inputStream = socket.getInputStream();
    StringBuilder result = new StringBuilder();
    do {
      result.append((char) inputStream.read());
    } while (inputStream.available() > 0);
    String output = result.toString();
    outputReceiver.output(output);
    inputStream.close();
  }

  @FinishBundle
  public void finishBundle() throws IOException {}

  @Teardown
  public void teardown() throws IOException {
    if (dataOutputStream != null) {
      dataOutputStream.flush();
      dataOutputStream.close();
      dataOutputStream = null;
    }
  }
}
