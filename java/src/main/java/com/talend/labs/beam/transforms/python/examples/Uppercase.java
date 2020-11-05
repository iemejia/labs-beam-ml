package com.talend.labs.beam.transforms.python.examples;

import com.talend.labs.beam.transforms.python.PythonTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Uppercase {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    String code =
        "element = input.split(',')\n" + "output = element[3].upper()\n";
    String requirements = "nltk==3.5";

    PCollection<String> jsons =
        p.apply(TextIO.read().from("/home/ismael/datasets/city_temperature/x*"))
            .apply(PythonTransform.of(code, requirements))
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
