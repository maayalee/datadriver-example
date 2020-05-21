package com.maayalee.dd.importers.loadgooglefitness;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DatasetsModel {
  private JsonArray array = new JsonArray();
  
  public DatasetsModel() {
  }

  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);

    String minStartTimeNs = element.getAsJsonObject().get("minStartTimeNs").getAsString();
    String maxEndTimeNs = element.getAsJsonObject().get("maxEndTimeNs").getAsString();
    String dataSourceId = element.getAsJsonObject().get("dataSourceId").getAsString();
    
    JsonArray points = element.getAsJsonObject().get("point").getAsJsonArray();
    for (int i = 0; i < points.size(); ++i) {
      JsonObject point = points.get(i).getAsJsonObject();

      JsonObject data = new JsonObject();
      data.addProperty("minStartTimeNs", minStartTimeNs);
      data.addProperty("maxEndTimeNs", maxEndTimeNs);
      data.addProperty("dataSourceId", dataSourceId);
      for (String key : point.keySet()) {
        JsonElement field = point.get(key);
        data.add(key, field);
      }
      array.add(data);
    }
  }

  public JsonArray getDatasets() {
    return array;
  }
}
