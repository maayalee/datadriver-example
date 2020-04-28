package com.maayalee.dd.importers.googlefitness;
import org.apache.commons.logging.Log;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregatedDatasetsModel {
  private static final Logger LOG = LoggerFactory.getLogger(GCSModel.class);
  
  private JsonArray array = new JsonArray();

  public AggregatedDatasetsModel() {
  }
  
  public void clear() {
    array = new JsonArray();
  }
  
  public void load(String jsonString) {
    LOG.info(jsonString);
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);
    JsonArray buckets = element.getAsJsonObject().get("bucket").getAsJsonArray();
    
    for (int i = 0; i < buckets.size(); ++i) {
      JsonObject bucket = buckets.get(i).getAsJsonObject();
      String startTimeMillis = bucket.get("startTimeMillis").getAsString();
      String endTimeMillis = bucket.get("endTimeMillis").getAsString();
      JsonArray datasets = bucket.get("dataset").getAsJsonArray();
      
      for (int j = 0; j < datasets.size(); ++j) {
        JsonObject dataset = datasets.get(j).getAsJsonObject();
        String dataSourceId = dataset.get("dataSourceId").getAsString();
        JsonArray points = dataset.get("point").getAsJsonArray();
        
        for (int k = 0; k < points.size(); ++k) {
          JsonObject point = points.get(k).getAsJsonObject();
          JsonObject data = new JsonObject();
          data.addProperty("startTimeMillis", startTimeMillis);
          data.addProperty("endTimeMillis", endTimeMillis);
          data.addProperty("dataSourceId", dataSourceId);
          for (String key : point.keySet()) {
            JsonElement field = point.get(key);
            data.add(key, field);
          }
          array.add(data);
        }
      }
    }
  }
  
  public JsonArray getDatasets() {
    return array;
  }
}
