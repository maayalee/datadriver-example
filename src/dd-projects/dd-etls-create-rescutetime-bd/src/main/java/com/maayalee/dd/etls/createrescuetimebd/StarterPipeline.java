package com.maayalee.dd.etls.createrescuetimebd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;


public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
    try {
      CreateJsonBDOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
          .as(CreateJsonBDOptions.class);
      LOG.info("appname:" + options.getAppName());

      Pipeline p = Pipeline.create(options);
      PCollection<String> lines = p.apply("ReadJSONLines", TextIO.read().from(
          options.getInputFilePattern()));
      
      NestedValueProvider<TableSchema, String> schemaProvider = NestedValueProvider.of(
          StaticValueProvider.of(options.getTableSchemaJSONPath()),
          createSerializableLoadSchema());
      PCollection<TableRow> tableRows = lines.apply("CreateBDRows", 
          ParDo.of(new CreateTableRow(schemaProvider)));
      
      Write<TableRow> write = BigQueryIO.writeTableRows().to(options.getOutputTable())
          .withSchema(loadSchema(options.getTableSchemaJSONPath()))
          .withTimePartitioning(new TimePartitioning().setType("DAY"))
          .withClustering(createClustering(options.getClusteringField()))
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
      tableRows.apply("WriteDB", write);
      p.run();
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
