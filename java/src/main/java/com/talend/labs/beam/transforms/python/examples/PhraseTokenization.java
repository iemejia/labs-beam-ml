package com.talend.labs.beam.transforms.python.examples;

import com.talend.labs.beam.transforms.python.PythonTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;

public class PhraseTokenization {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    PCollection<String> jsons =
        p.apply(GenerateSequence.from(0).to(100))
            .apply(ToString.elements())
            .apply(ParDo.of(new JsonifyFn()));

    String code =
            "import json\n"
                    + "element = json.loads(input)\n"
                    + "from nltk.tokenize import sent_tokenize\n"
                    + "phrases = sent_tokenize(element['sentence'])\n"
                    + "output = [element['book'] + ': ' + x for x in phrases]\n";
    String requirements = "nltk==3.5";
    jsons.apply(PythonTransform.of(code, requirements)).apply(ParDo.of(new PrintFn<>()));

    p.run().waitUntilFinish();
  }

  private static class JsonifyFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> out) {
      String json =
          "{ \"book\":\""
              + element
              + "\", \"sentence\":\"All work and no play makes jack a dull boy, all work and no play. " +
                  "All work and no play makes jack a dull boy, all work and no play. " +
                  "All work and no play makes jack a dull boy, all work and no play.\"}";
      out.output(json);
    }
  }

  private static class PrintFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> out) {
      System.out.println("JAVA OUTPUT: " + element);
      out.output(element);
    }
  }

}
