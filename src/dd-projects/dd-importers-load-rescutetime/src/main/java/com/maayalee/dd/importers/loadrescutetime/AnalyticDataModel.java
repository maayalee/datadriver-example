package com.maayalee.dd.importers.loadrescutetime;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AnalyticDataModel {
  public static String[] PRODUCTIVITY_STRINGS = new String[] { "very distracting", "distracting", "neutral",
      "productive", "very productive" };

  public AnalyticDataModel() {
  }

  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);
    JsonArray row_headers = element.getAsJsonObject().get("row_headers").getAsJsonArray();

    JsonArray rows = element.getAsJsonObject().get("rows").getAsJsonArray();

    for (int i = 0; i < rows.size(); ++i) {
      JsonArray elements = rows.get(i).getAsJsonArray();
      JsonObject data = new JsonObject();
      for (int j = 0; j < elements.size(); ++j) {
        String name = row_headers.get(j).getAsString().toLowerCase().replace(" ", "_");
        String value = elements.get(j).getAsString();
        if (name.equals("productivity")) {
          int productivity = Integer.parseInt(value);
          data.addProperty("productivity_string", PRODUCTIVITY_STRINGS[productivity + 2]);
        }
        data.addProperty(name, value);
      }
      array.add(data);
    }
  }

  public JsonArray getAnalyticDatas() {
    return array;
  }

  private JsonArray array = new JsonArray();
}
