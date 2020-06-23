package com.maayalee.dd.etls.creategooglefitnessbd;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;

public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
    try {
      CreateJsonBDOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
          .as(CreateJsonBDOptions.class);
      LOG.info("appname:" + options.getAppName());

      Pipeline pipeline = Pipeline.create(options);
      PCollection<String> aggregatedDatasetsLines = pipeline.apply("ReadJSONLines - AggregatedDatasets", 
          TextIO.read().from(options.getInputAggregatedDatasetsFilePattern()));

      NestedValueProvider<TableSchema, String> aggregatedDatasetSchemaProvider = NestedValueProvider.of(
          StaticValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath()),
          createSerializableLoadSchema());
      PCollection<TableRow> tableRows = aggregatedDatasetsLines.apply("CreateBDRows - AggregatedDatasets", 
          ParDo.of(new CreateTableRow(aggregatedDatasetSchemaProvider)));

      Clustering clustering = createClustering(options.getClusteringField());
      Write<TableRow> aggregatedDatasetsWrite = BigQueryIO.writeTableRows().to(
          options.getOutputAggregatedDatasetsTable())
          .withSchema(loadSchema(options.getTableSchemaAggregatedDatasetsJSONPath()))
          .withTimePartitioning(new TimePartitioning().setType("DAY"))
          .withClustering(clustering)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
      tableRows.apply("WriteDB - AggregatedDatasets", aggregatedDatasetsWrite);

      NestedValueProvider<TableSchema, String> sessionSchemaProvider = NestedValueProvider.of(
          StaticValueProvider.of(options.getTableSchemaSessionsJSONPath()),
          createSerializableLoadSchema());

      PCollection<String> sessionlines = pipeline.apply("ReadJSONLines - Sessions", 
          TextIO.read().from(options.getInputSessionsFilePattern()));
      tableRows = sessionlines.apply("CreateBDRows - Sessions", 
          ParDo.of(new CreateTableRow(sessionSchemaProvider)));
      tableRows = tableRows.apply("FilteringRows - Sessions", 
          ParDo.of(new DivideTime(options.getBeginTime(), options.getEndTime())));

      Write<TableRow> sessionWrite = BigQueryIO.writeTableRows().to(options.getOutputSessionsTable())
          .withSchema(loadSchema(options.getTableSchemaSessionsJSONPath()))
          .withTimePartitioning(new TimePartitioning().setType("DAY"))
          .withClustering(clustering)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
      tableRows.apply("WriteDB - Sessions", sessionWrite);
      pipeline.run();
    } catch (NullPointerException e) {
      LOG.error("NullPointerException occured: " + getStackTrace(e));
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      LOG.error("Occured exception: " + sw.toString());
    }
  }

  private static TableSchema loadSchema(String schemaPath) {
    SchemaParser parser = new SchemaParser();
    try {
      JSONObject jsonSchema = parser.parseSchema(schemaPath);
      JSONArray fields = jsonSchema.getJSONArray("fields");
      List<TableFieldSchema> schemaFields = new ArrayList<TableFieldSchema>();
      for (int i = 0; i < fields.length(); ++i) {
        JSONObject field = fields.getJSONObject(i);
        TableFieldSchema schema = new TableFieldSchema().setName(field.getString("name"));
        schema.setType(field.getString("type"));
        if (field.getString("mode").length() > 0) {
          schema.setMode(field.getString("mode"));
        }
        schemaFields.add(schema);
      }
      return new TableSchema().setFields(schemaFields);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } 
  }

   private static Clustering createClustering(String field) {
    List<String> clusteringFields = new ArrayList<String>();
    clusteringFields.add(field);
    Clustering clustering = new Clustering();
    return clustering.setFields(clusteringFields);
  }


  @SuppressWarnings("serial")
  private static SerializableFunction<String, TableSchema> createSerializableLoadSchema() {
    return new SerializableFunction<String, TableSchema>() {
      @Override
      public TableSchema apply(String jsonPath) {
        return loadSchema(jsonPath);
      }
    };
  }

  private static String getStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
