/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// package com.google.cloud.pso.pipeline;
package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.api.services.bigquery.model.Clustering;

import com.google.common.annotations.VisibleForTesting;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;

/**
 * The {@link PubsubToBigQueryDynamicDestinations} is a streaming pipeline which dynamically routes
 * messages to their output location using an attribute within the Pub/Sub message header. This
 * pipeline requires any tables which will be routed to, to be defined prior to execution.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub subscription must exist prior to pipeline execution.
 *   <li>The BigQuery output tables routed to must be created prior to pipeline execution.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT_ID
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/pubsub-to-bigquery-dynamic-destinations
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.pso.pipeline.PubsubToBigQueryDynamicDestinations \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * --subscription=SUBSCRIPTION \
 * --outputTableProject=PROJECT \
 * --outputTableDataset=DATASET"
 * --jsonSchema="../monorepo/infrastructure/inspector-dynamic-dataflow-job/bulk_schema.json"
 * </pre>
 */
public class PubsubToBigQueryDynamicDestinations {

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {

    @Description("The Pub/Sub subscription to read messages from")
    @Required
    String getSubscription();

    void setSubscription(String value);

    @Description(
        "The name of the attribute which will contain the table name to route the message to.")
    @Required
    String getOutputTableProject();

    void setOutputTableProject(String value);

    @Description(
        "The name of the attribute which will contain the table name to route the message to.")
    @Required
    String getOutputTableDataset();

    void setOutputTableDataset(String value);

    @Description(
      "The path to Json File containing the schema"
    )
    @Required
    String getJsonSchema();

    void setJsonSchema(String value);

    // @Description(
    //   "The path to Json File containing partioning config"
    // )
    // String getJsonPartition();

    // void setJsonPartition(String value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubsubToBigQueryDynamicDestinations#run(Options)} method to start the pipeline and invoke
   * {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);

    run(options);
  }


  private static String parseJsonFile(String path) {
    String json = null;
    try {
      json = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read json schema: " + json, e);
    }

    return json;
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Retrieve non-serializable parameters
    String outputTableProject = options.getOutputTableProject();
    String outputTableDataset = options.getOutputTableDataset();

    String jsonSchemaPath = options.getJsonSchema();
    // String jsonPartitioningPath = options.getJsonPartition();

    String jsonSchema = parseJsonFile(jsonSchemaPath);
    // String jsonPartition = parseJsonFile(jsonPartitioningPath);

    // Build & execute pipeline
    pipeline
        .apply(
            "ReadMessages",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubscription()))
        .apply(
            "WriteToBigQuery",
            BigQueryIO.<PubsubMessage>write()
                .withFormatFunction(
                    (PubsubMessage msg) -> convertJsonToTableRow(new String(msg.getPayload())))
                .withJsonSchema(jsonSchema)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withMethod(Method.STREAMING_INSERTS)
                 .to(
                    input ->
                        getTableDestination(
                            input,
                            outputTableProject,
                            outputTableDataset)));

    return pipeline.run();
  }

  /**
   * OUTDATED DESCRIPTION
   * Retrieves the {@link TableDestination} for the {@link PubsubMessage} by extracting and
   * formatting the value of the {@code tableNameAttr} attribute. If the message is null, a {@link
   * RuntimeException} will be thrown because the message is unable to be routed.
   *
   * @param value The message to extract the table name from.
  
   * @param outputProject The project which the table resides.
   * @param outputDataset The dataset which the table resides.
   * @return The destination to route the input message to.
   */
  @VisibleForTesting
   static TableDestination getTableDestination(
      ValueInSingleWindow<PubsubMessage> value,
      String outputProject,
      String outputDataset) {
    PubsubMessage message = value.getValue();

    String s = null;
    try {
      s = new String(message.getPayload(), StandardCharsets.UTF_8);
    } catch (Exception e){
      throw new RuntimeException("Cannot Serialize schema json");
    }
    JSONObject json = new JSONObject(s);
    String schemaId = json.getString("schemaId");
    String env = json.getString("env");


    // TimePartitioning timePartitioning = new TimePartitioning();
    // timePartitioning.setField("receivedAt");
    // timePartitioning.setType("DAY");
    // timePartitioning.setRequirePartitionFilter(true);


    TableDestination destination;
    if (schemaId != null && env != null) {
      destination =
          new TableDestination(
              String.format(
                  "%s:%s.customer_bulk_events_%s_%s",
                  outputProject, outputDataset, schemaId, env),
              null);
              
              // new Clustering().setFields(Arrays.asList("foldedAt", "eventName"))
          
    } else {
      throw new RuntimeException(
          String.format("Cannot retrieve the dynamic table destination of an null message: %s", message.toString()));
      // destination = String.format(
      //             "%s:%s.customer_bulk_events_unknown",
      //             outputProject, outputDataset),
      //         null)
    }

    return destination;
  }

  /**
   * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
   * RuntimeException} will be thrown.
   *
   * @param json The JSON string to parse.
   * @return The parsed {@link TableRow} object.
   */
  @VisibleForTesting
  static TableRow convertJsonToTableRow(String json) {
    TableRow row;
    // Parse the JSON into a {@link TableRow} object.
    try (InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }

    return row;
  }
}