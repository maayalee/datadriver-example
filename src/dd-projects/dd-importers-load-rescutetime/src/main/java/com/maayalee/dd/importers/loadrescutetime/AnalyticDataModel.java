package com.maayalee.dd.importers.loadrescutetime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AnalyticDataModel {
  private static final Logger LOG = LoggerFactory.getLogger(App.class);
  
  private JsonArray array = new JsonArray();
  private String userId;
  private String loadTimezone;
  private String outputDate;
  private String outputTimezone;

  public AnalyticDataModel(String userId, String loadTimezone, String outputDate, String outputTimezone) {
    this.userId = userId;
    this.loadTimezone = loadTimezone;
    this.outputDate = outputDate;
    this.outputTimezone = outputTimezone;
  }

  public void clear() {
    array = new JsonArray();
  }

  public void load(String jsonString) throws ParseException {
    JsonParser jsonParser = new JsonParser();
    JsonElement element = jsonParser.parse(jsonString);
    JsonArray row_headers = element.getAsJsonObject().get("row_headers").getAsJsonArray();
    JsonArray rows = element.getAsJsonObject().get("rows").getAsJsonArray();

    // 로그의 시간을 어떤 타임존 시간대로 처리할지 설정
    SimpleDateFormat loadDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    loadDateFormat.setTimeZone(TimeZone.getTimeZone(loadTimezone));
    
    SimpleDateFormat outputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    outputDateFormat.setTimeZone(TimeZone.getTimeZone(outputTimezone));
    Date targetDate = outputDateFormat.parse(outputDate);
    long targetDateStartTimestamp = targetDate.getTime();
    long targetDateEndTimestamp = targetDate.getTime() + 86400000;
    LOG.info("target timestamp range:" + targetDateStartTimestamp + "-" + targetDateEndTimestamp);

    
    for (int i = 0; i < rows.size(); ++i) {
      JsonArray elements = rows.get(i).getAsJsonArray();
      JsonObject data = new JsonObject();
      data.addProperty("user_id", userId);
      
      boolean matchedDate = false;
      for (int j = 0; j < elements.size(); ++j) {  
        String name = row_headers.get(j).getAsString().toLowerCase().replace(" ", "_");
        String value = elements.get(j).getAsString();
        if (name.equals("date")) {
          Date logDate = loadDateFormat.parse(value);
          long timestamp = logDate.getTime();
          // 레스큐타임은 유저가 설정한 타임존 기준으로 데이터를 내보내주므로 내가 원하는 UTC 시간대 기준 날짜에 속하는 경우에만 저장하도록 한다
          if (timestamp >= targetDateStartTimestamp && timestamp < targetDateEndTimestamp) {
            matchedDate = true;
            data.addProperty("date_timestamp_millis", String.valueOf(timestamp));
          }
        }
        data.addProperty(name, value);
      }
      
      if (matchedDate) {
        array.add(data);
      }
    }
  }

  public JsonArray getAnalyticDatas() {
    return array;
  }
}
