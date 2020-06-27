package com.maayalee.dd.etls.creategooglefitnessbd;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.util.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaParser {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaParser.class);

  public TableSchema parseSchema(String pathToJSON) {
    try {
      ReadableByteChannel readableByteChannel = FileSystems.open(FileSystems.matchNewResource(pathToJSON, false));

      String json = new String(StreamUtils.getBytesWithoutClosing(Channels.newInputStream(readableByteChannel)));

      JSONObject jsonSchema = new JSONObject(json);
      JSONArray fields = jsonSchema.getJSONArray("fields");
      List<TableFieldSchema> schemaFields = new ArrayList<TableFieldSchema>();
      for (int i = 0; i < fields.length(); ++i) {
        JSONObject field = fields.getJSONObject(i);
        TableFieldSchema schema = new TableFieldSchema().setName(field.getString("name"));
        schema.setType(field.getString("type"));
        schemaFields.add(schema);
      }
      return new TableSchema().setFields(schemaFields);
    } catch (Exception e) {
      LOG.error("Exception occured: " + getStackTrace(e));
      return new TableSchema();
    }
  }
  
  private static String getStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}