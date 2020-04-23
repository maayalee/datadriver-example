package com.maayalee.dd.importers.googlefitness;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class DatasourcesModel {
  public DatasourcesModel() {
  }
  
  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);
    array = element.getAsJsonObject().get("dataSource").getAsJsonArray();
  }

  public JsonArray getDatasources() {
    return array;
  }

  private JsonArray array = new JsonArray();
}
