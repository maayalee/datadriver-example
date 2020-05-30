package com.maayalee.dd.importers.loadgooglefitness;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DatasourcesModel {
  private JsonArray array = new JsonArray();
  private String userId;
  
  public DatasourcesModel(String userId) {
    this.userId = userId;
  }
  
  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);
    JsonArray dataSource = element.getAsJsonObject().get("dataSource").getAsJsonArray();
    
    for (int i = 0; i < dataSource.size(); ++i) {
      JsonObject data = dataSource.get(i).getAsJsonObject().deepCopy();
      data.addProperty("user_id", userId);
      array.add(data);
    }
  }

  public JsonArray getDatasources() {
    return array;
  }
}
