package com.maayalee.dd.importers.googlefitness;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SessionsModel {
  private static Map<String, String> ACTIVITY_TYPE_STRINGS = new HashMap<String, String>();
  
  public SessionsModel() {
    ACTIVITY_TYPE_STRINGS.put("7", "Walking*");
    ACTIVITY_TYPE_STRINGS.put("72", "Sleeping");
  }
  
  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);

    JsonArray sessions = element.getAsJsonObject().get("session").getAsJsonArray();
    for (int i = 0; i < sessions.size(); ++i) {
      JsonObject session = sessions.get(i).getAsJsonObject();

      JsonObject data = new JsonObject();
      for (String key : session.keySet()) {
        JsonElement field = session.get(key);
        data.add(key, field);
      }
      data.addProperty("activityType_String", ACTIVITY_TYPE_STRINGS.get(data.get("activityType").getAsString()));
      array.add(data);
    }
  }
  
  public JsonArray getSessions() {
    return array;
  }
  
  private JsonArray array = new JsonArray();
}
