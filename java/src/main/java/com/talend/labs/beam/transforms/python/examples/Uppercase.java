package com.talend.labs.beam.transforms.python.examples;

import com.talend.labs.beam.transforms.python.PythonTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Uppercase {
  /** Specific pipeline options. */
  public interface UppercasePipelineOptions extends PipelineOptions {
    @Description("Input path")
    String getInputPath();

    void setInputPath(String path);

    @Description("Server invoker path")
    String getServerInvokerPath();

    void setServerInvokerPath(String path);
  }

  public static void main(String[] args) {
    UppercasePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(UppercasePipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> input =
        (options.getInputPath() == null)
            ? p.apply(Create.of("Africa,Algeria,,Algiers,1,1,1995,64.2"))
            : p.apply(TextIO.read().from(options.getInputPath()));

    String code = "element = input.split(',')\n" + "output = element[3].upper()\n";
    String requirements = "nltk==3.5";

    PCollection<String> output =
        input
            .apply(PythonTransform.of(code, requirements, options.getServerInvokerPath()))
            .apply(ParDo.of(new PrintFn<>()));

    p.run().waitUntilFinish();
  }

  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA OUTPUT: " + element);
      out.output(element);
    }
  }
}
