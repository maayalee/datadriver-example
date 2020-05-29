package com.maayalee.dd.etls.creategooglefitnessbd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

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
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.maayalee.dd.etls.creategooglefitnessbd.TableSchemaDTO.TableField;
import com.google.api.client.util.DateTime;

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
  
  @SuppressWarnings("serial")
  static class AdjustTimeRange extends DoFn<TableRow, TableRow> {
    private static final Long THIRTY_MINUTES = 1800000L;
    
    public AdjustTimeRange(ValueProvider<String> beginTime, ValueProvider<String> endTime) {
      this.beginTime = beginTime;
      this.endTime = endTime;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow tableRow = c.element();
      try {
        TableRow newTableRow = tableRow.clone();
        Long beginTimeOfDate = DateTime.parseRfc3339(beginTime.get()).getValue();
        Long endTimeOfDate = DateTime.parseRfc3339(endTime.get()).getValue();
        Long startTime = Long.parseLong((String)tableRow.get("startTimeMillis"));
        Long endTime = Long.parseLong((String)tableRow.get("endTimeMillis"));

        // 이틀에 걸쳐 중복되는 데이터는 범위에 해당하는 날짜마다 데이터를 돌려준다. 따라서 로드하는 날짜의 범위안에 있는 시간으로 변경해준다. 
        if (startTime < beginTimeOfDate) {
          newTableRow.set("startTimeMillis", beginTimeOfDate);
          startTime = beginTimeOfDate;
        }
        if (endTime > endTimeOfDate) {
          newTableRow.set("endTimeMillis", endTimeOfDate);
          endTime = endTimeOfDate;
        }
        // 다양한 타임존 기준으로 측정을 위해 데이터의 범위를 30분 단위로 분리해서 저장한다. 
        if ((endTime - startTime) > THIRTY_MINUTES) {
          Long currentStartTime = startTime;
          Long currentEndTime = startTime + THIRTY_MINUTES;
          while (true) {
            TableRow divideTableRow = newTableRow.clone();
            divideTableRow.set("startTimeMillis", currentStartTime);
            divideTableRow.set("endTimeMillis", currentEndTime > endTime ? endTime : currentEndTime);
            c.output(divideTableRow);

            if (currentEndTime > endTime) {
              break;
            }
            currentStartTime += THIRTY_MINUTES;
            currentEndTime += THIRTY_MINUTES;
          }
        }
        else {
          c.output(newTableRow);
        }
        
      } catch (Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        LOG.error(String.format("Occured exception: %s", sw.toString()));
      }
    }

    private ValueProvider<String> beginTime;
    private ValueProvider<String> endTime;
  }

  public CreateJsonBDRunner() {
  }

  public static void start(CreateJsonBDOptions options) {
    try {
 
      Pipeline p = Pipeline.create(options);
      PCollection<String> lines = p.apply("ReadJSONLines - AggregatedDatasets", TextIO.read().from(options.getInputAggregatedDatasetsFilePattern()));
      CreateTableRow createTableRow = new CreateTableRow(
          NestedValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath(), createLoadSchmeaFunction()));
      AdjustTimeRange adjustTimeRange = new AdjustTimeRange(options.getBeginTime(), options.getEndTime());

      PCollection<TableRow> tableRows = lines.apply("CreateBDRows - AggregatedDatasets", ParDo.of(createTableRow));
      tableRows.apply("WriteDB - AggregatedDatasets",
          BigQueryIO.writeTableRows().to(options.getOutputAggregatedDatasetsTable())
              .withSchema(NestedValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath(), createLoadSchmeaFunction()))
              .withTimePartitioning(new TimePartitioning().setType("DAY"))
              .withClustering(NestedValueProvider.of(options.getTableSchemaAggregatedDatasetsJSONPath(), createLoadClustering()).get())
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
      
      lines = p.apply("ReadJSONLines - Sessions", TextIO.read().from(options.getInputSessionsFilePattern()));
      createTableRow = new CreateTableRow(
          NestedValueProvider.of(options.getTableSchemaSessionsJSONPath(), createLoadSchmeaFunction()));
      tableRows = lines.apply("CreateBDRows - Sessions", ParDo.of(createTableRow));
      tableRows = tableRows.apply("FilteringRows - Sessions", ParDo.of(adjustTimeRange));
      tableRows.apply("WriteDB - Sessions",
          BigQueryIO.writeTableRows().to(options.getOutputSessionsTable())
              .withSchema(NestedValueProvider.of(options.getTableSchemaSessionsJSONPath(), createLoadSchmeaFunction()))
              .withTimePartitioning(new TimePartitioning().setType("DAY"))
              .withClustering(NestedValueProvider.of(options.getTableSchemaSessionsJSONPath(), createLoadClustering()).get())
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
  
  @SuppressWarnings("serial")
  private static SerializableFunction<String, Clustering> createLoadClustering() {
    return new SerializableFunction<String, Clustering>() {
      @Override
      public Clustering apply(String jsonPath) {
        Clustering result = new Clustering();
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
            fieldDTO.isClustering = field.getBoolean("is_clustering");
            dto.fields.add(fieldDTO);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        List<String> fields = new ArrayList<String>();
        for (TableField field : dto.fields) {
          if (field.isClustering) {
            fields.add(field.name);
          }
        }
        return result.setFields(fields);
      }
    };
  }
}
