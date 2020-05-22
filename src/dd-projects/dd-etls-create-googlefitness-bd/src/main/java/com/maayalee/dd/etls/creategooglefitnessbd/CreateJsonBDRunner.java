package com.maayalee.dd.etls.creategooglefitnessbd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.flattener.StringEscapePolicy;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.maayalee.dd.etls.creategooglefitnessbd.TableSchemaDTO.TableField;

public class CreateJsonBDRunner {
  private static final Logger LOG = LoggerFactory.getLogger(CreateJsonBDRunner.class);
  
  @SuppressWarnings("serial")
  static class CreateTableRow extends DoFn<String, TableRow> {
    public CreateTableRow(ValueProvider<TableSchema> schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String jsonString = c.element();
      TableRow tableRow = new TableRow();
      try {
        JsonFlattener flattener = new JsonFlattener(jsonString).withStringEscapePolicy(StringEscapePolicy.DEFAULT);
        Map<String, Object> flattenJson = flattener.flattenAsMap();
        for (Map.Entry<String, Object> entry : flattenJson.entrySet()) {
          String key = entry.getKey().replaceAll("[ .]", "_").replaceAll("[ \\[\\]]", "").replaceAll("[ \\(\\)]", "");
          Object value = entry.getValue();
          LOG.info("Key : " + key + " Value : " + value);

          for (int i = 0; i < schema.get().getFields().size(); ++i) {
            TableFieldSchema field = schema.get().getFields().get(i);
            if (field.getName().equals(key)) {
              tableRow.set(key, value);
            }
          }
        }
        c.output(tableRow);
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        LOG.error(String.format("Occured exception: %s, row: %s", sw.toString(), jsonString));
      }
    }

    private ValueProvider<TableSchema> schema;
  }

  public CreateJsonBDRunner() {
  }

  public static void start(CreateJsonBDOptions options) {
    try {
      Pipeline p = Pipeline.create(options);
      
      PCollection<String> lines = p.apply("ReadJSONLines - AggregatedDatasets", TextIO.read().from(options.getInputAggregatedDatasetsFilePattern()));
      CreateTableRow createTableRow = new CreateTableRow(
          NestedValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath(), createLoadSchmeaFunction()));
      PCollection<TableRow> tableRows = lines.apply("CreateBDRows - AggregatedDatasets", ParDo.of(createTableRow));
      tableRows.apply("WriteDB - AggregatedDatasets",
          BigQueryIO.writeTableRows().to(options.getOutputAggregatedDatasetsTable())
              .withSchema(NestedValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath(), createLoadSchmeaFunction()))
              .withTimePartitioning(new TimePartitioning().setType("DAY"))
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
      
      lines = p.apply("ReadJSONLines - Datasets", TextIO.read().from(options.getInputDatasetsFilePattern()));
      createTableRow = new CreateTableRow(
          NestedValueProvider.of(options.getTableSchemaDatasetsJSONPath(), createLoadSchmeaFunction()));
      tableRows = lines.apply("CreateBDRows - Datasets", ParDo.of(createTableRow));
      tableRows.apply("WriteDB - Datasets",
          BigQueryIO.writeTableRows().to(options.getOutputDatasetsTable())
              .withSchema(NestedValueProvider.of(options.getTableSchemaDatasetsJSONPath(), createLoadSchmeaFunction()))
              .withTimePartitioning(new TimePartitioning().setType("DAY"))
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
      
      lines = p.apply("ReadJSONLines - Sessions", TextIO.read().from(options.getInputSessionsFilePattern()));
      createTableRow = new CreateTableRow(
          NestedValueProvider.of(options.getTableSchemaSessionsJSONPath(), createLoadSchmeaFunction()));
      tableRows = lines.apply("CreateBDRows - Sessions", ParDo.of(createTableRow));
      tableRows.apply("WriteDB - Sessions",
          BigQueryIO.writeTableRows().to(options.getOutputSessionsTable())
              .withSchema(NestedValueProvider.of(options.getTableSchemaSessionsJSONPath(), createLoadSchmeaFunction()))
              .withTimePartitioning(new TimePartitioning().setType("DAY"))
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
      p.run();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      LOG.error("Occured exception: " + sw.toString());
    }
  }

  @SuppressWarnings("serial")
  private static SerializableFunction<String, TableSchema> createLoadSchmeaFunction() {
    return new SerializableFunction<String, TableSchema>() {
      @Override
      public TableSchema apply(String jsonPath) {
        SchemaParser parser = new SchemaParser();
        TableSchemaDTO dto = new TableSchemaDTO();
        try {
          JSONObject jsonSchema = parser.parseSchema(jsonPath);
          String name = jsonSchema.getString("name");
          JSONArray fields = jsonSchema.getJSONArray("fields");

          dto.name = name;
          for (int i = 0; i < fields.length(); ++i) {
            JSONObject field = fields.getJSONObject(i);
            TableSchemaDTO.TableField fieldDTO = new TableSchemaDTO.TableField();
            fieldDTO.name = field.getString("name");
            fieldDTO.type = field.getString("type");
            fieldDTO.mode = field.getString("mode");
            dto.fields.add(fieldDTO);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        for (TableField field : dto.fields) {
          TableFieldSchema schema = new TableFieldSchema().setName(field.name);
          schema.setType(field.type);
          if (field.mode.length() > 0) {
            schema.setMode(field.mode);
          }
          fields.add(schema);
        }
        return new TableSchema().setFields(fields);
      }
    };
  }
}
