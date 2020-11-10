package com.talend.labs.beam.transforms.python;

import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.ExpansionResolver;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;

public class PythonTransform extends PTransform<PCollection<String>, PCollection<String>> {

  private final String host;
  private final Integer port;
  private final String code;
  private final String requirements;
  private final String serverInvokerPath;

  private PythonTransform(
      String host,
      @Nullable Integer port,
      String code,
      String requirements,
      String serverInvokerPath) {
    this.host = host;
    this.port = port;
    this.code = code;
    this.requirements = requirements;
    this.serverInvokerPath = serverInvokerPath;
  }

  // TODO might we do requirements a path better so it gets the requirements from some FS?

  /**
   * @param code Python script to be executed
   * @param requirements contents of requirements.txt file to setup a virtualenv
   * @param serverInvokerPath path to Python server invoker
   * @return
   */
  public static PythonTransform of(String code, String requirements, String serverInvokerPath) {
    return new PythonTransform("localhost", null, code, requirements, serverInvokerPath);
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    // Uncomment to test with SDK Harness
    //    ByteString pythonTransformPayload =
    //        ExpansionResolver.getPythonPTransformCode(
    //            input, "talend:labs:ml:genreclassifier:python:v1", new byte[0], "localhost:9097");
    //    return input.apply(ParDo.of(new InvokeViaSdkHarnessDoFn(pythonTransformPayload)));
    return input.apply(
        ParDo.of(new InvokeViaSocketsDoFn(host, port, code, requirements, serverInvokerPath)));
  }
}
