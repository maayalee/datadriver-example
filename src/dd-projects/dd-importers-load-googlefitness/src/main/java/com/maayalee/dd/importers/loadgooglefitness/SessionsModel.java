package com.maayalee.dd.importers.loadgooglefitness;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SessionsModel {
  private static Map<String, String> ACTIVITY_TYPE_STRINGS = new HashMap<String, String>();
  
  private JsonArray array = new JsonArray();
  private String userId;
  
  public SessionsModel(String userId) {
    ACTIVITY_TYPE_STRINGS.put("7", "Walking*");
    ACTIVITY_TYPE_STRINGS.put("72", "Sleeping");
    
    this.userId = userId;
  }
  
  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);

    JsonArray sessions = element.getAsJsonObject().get("session").getAsJsonArray();
    for (int i = 0; i < sessions.size(); ++i) {
      JsonObject data = sessions.get(i).getAsJsonObject().deepCopy();
      data.addProperty("user_id", userId);
      data.addProperty("activityType_String", ACTIVITY_TYPE_STRINGS.get(data.get("activityType").getAsString()));
      array.add(data);
    }
  }
  
  public JsonArray getSessions() {
    return array;
  }
  
}
