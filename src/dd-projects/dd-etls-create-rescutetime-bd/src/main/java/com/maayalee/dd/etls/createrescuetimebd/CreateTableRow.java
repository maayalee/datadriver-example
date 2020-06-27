package com.maayalee.dd.etls.createrescuetimebd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.flattener.StringEscapePolicy;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

@SuppressWarnings("serial")
class CreateTableRow extends DoFn<String, TableRow> {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
    public static String[] PRODUCTIVITY_STRINGS = new String[] { "very distracting", "distracting", "neutral",
      "productive", "very productive" };

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
          //String key = entry.getKey().replaceAll("[ .]", "_").replaceAll("[ \\[\\]]", "").replaceAll("[ \\(\\)]", "");
          String key = entry.getKey().replace(".", "_").replaceAll("[\\[\\]]", "");
          Object value = entry.getValue();
          LOG.info("Key : " + key + " Value : " + value);

          for (int i = 0; i < schema.get().getFields().size(); ++i) {
            TableFieldSchema field = schema.get().getFields().get(i);
            if (field.getName().equals(key)) {
              tableRow.set(key, value);
            }
          }
        }
        if (tableRow.containsKey("productivity")) {
          int productivity = Integer.parseInt(tableRow.get("productivity").toString());
          tableRow.set("productivity_string", PRODUCTIVITY_STRINGS[productivity + 2]);
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