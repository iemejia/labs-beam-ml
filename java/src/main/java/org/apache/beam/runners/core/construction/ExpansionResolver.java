package org.apache.beam.runners.core.construction;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A class to get the Python Transform payload from an ExpansionService It is in the beam package
 * because of access related issues.
 */
public class ExpansionResolver {

    // Pay more attention to MEEEEEEEEEEEEEE

  private static final String EXPANDED_TRANSFORM_BASE_NAME = "external";
  private static final String IMPULSE_PREFIX = "IMPULSE";

  public static <T> ByteString getPythonPTransformCode(
      PCollection<T> input, String urn, byte[] payload, String endpoint) {
    ExpansionResponse response = getExpansion(input, urn, payload, endpoint);
    if (!Strings.isNullOrEmpty(response.getError())) {
      throw new RuntimeException(String.format("expansion service error: %s", response.getError()));
    }

    RunnerApi.PTransform expandedTransform = response.getTransform();
    String responseUrn = expandedTransform.getSpec().getUrn();
    String expandedTransformId = expandedTransform.getSubtransforms(0);
    RunnerApi.PTransform pTransform =
        response.getComponents().getTransformsMap().get(expandedTransformId);
    ByteString responsePayload = pTransform.getSpec().getPayload();
    return responsePayload;
  }

  public static <T> ExpansionApi.ExpansionResponse getExpansion(
      PCollection<T> input, String urn, byte[] payload, String endpoint) {
    Pipeline p = input.getPipeline();

    Integer namespaceIndex = 0;
    // TODO optimize to have only one client per JVM
    final DefaultExpansionServiceClientFactory DEFAULT =
        new DefaultExpansionServiceClientFactory(
            endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

    SdkComponents components = SdkComponents.create(p.getOptions());
    RunnerApi.PTransform.Builder ptransformBuilder =
        RunnerApi.PTransform.newBuilder()
            .setUniqueName(EXPANDED_TRANSFORM_BASE_NAME + namespaceIndex)
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn(urn)
                    .setPayload(ByteString.copyFrom(payload))
                    .build());

    ImmutableMap.Builder<PCollection, String> externalPCollectionIdMapBuilder =
        ImmutableMap.builder();
    for (Map.Entry<TupleTag<?>, PValue> entry : input.expand().entrySet()) {
      if (entry.getValue() instanceof PCollection<?>) {
        try {
          String id = components.registerPCollection((PCollection) entry.getValue());
          externalPCollectionIdMapBuilder.put((PCollection) entry.getValue(), id);
          ptransformBuilder.putInputs(entry.getKey().getId(), id);
          AppliedPTransform<?, ?, ?> fakeImpulse =
              AppliedPTransform.of(
                  String.format("%s_%s", IMPULSE_PREFIX, entry.getKey().getId()),
                  PBegin.in(p).expand(),
                  ImmutableMap.of(entry.getKey(), entry.getValue()),
                  Impulse.create(),
                  p);
          // using fake Impulses to provide inputs
          components.registerPTransform(fakeImpulse, Collections.emptyList());
        } catch (IOException e) {
          throw new RuntimeException(
              String.format("cannot register component: %s", e.getMessage()));
        }
      }
    }

    String namespace = "External" + namespaceIndex;

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(components.toComponents())
            .setTransform(ptransformBuilder.build())
            .setNamespace(namespace)
            .build();
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(endpoint).build();
    ExpansionApi.ExpansionResponse response =
        DEFAULT.getExpansionServiceClient(apiServiceDescriptor).expand(request);
    return response;
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply(Create.of("1", "2", "3"));
    ByteString pythonTransformCode =
        getPythonPTransformCode(
            input, "talend:labs:ml:genreclassifier:python:v1", new byte[0], "localhost:9097");
    System.out.println(pythonTransformCode.toString());
  }
}
