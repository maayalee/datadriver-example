package com.maayalee.dd.importers.googlefitness;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConstants;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.DateTime;
import com.google.api.services.fitness.model.DataType;
import com.google.api.services.fitness.model.DataTypeField;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

// HTTP요청시 DATA STORE DIR의 파일을 다른 프로세스와 공유하면 scope가 허용안된 인증 확인 정보 문제 때문에 Insufficent Permission 에러 응답을 받을 수 있다.
public class GoogleFitnessPresenter {
  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  // AGGREGATION 요청할 데이터 타입 (https://developers.google.com/android/reference/com/google/android/gms/fitness/data/DataType)
  private static final String[] aggregatedDataTypeNames = {
      "com.google.step_count.delta",
      "com.google.distance.delta",
      "com.google.calories.expended",
      "com.google.heart_rate.bpm",
  };
  
  private DatasourcesModel datasources;
  private DatasetsModel datasets;
  private AggregatedDatasetsModel aggregatedDatasetsModel;
  private SessionsModel sessions;

  public GoogleFitnessPresenter(DatasourcesModel datasources, DatasetsModel datasets, AggregatedDatasetsModel aggregatedDatasets, SessionsModel sessions) {
    this.datasources = datasources;
    this.datasets = datasets;
    this.aggregatedDatasetsModel = aggregatedDatasets;
    this.sessions = sessions;
  }

  public void load(String beginTime, String endTime, String accessToken) throws Exception {
    String url = String.format("https://www.googleapis.com/fitness/v1/users/me/sessions?startTime=%s&endTime=%s",
        URLEncoder.encode(beginTime, "utf-8"), URLEncoder.encode(endTime, "utf-8"));
    sessions.load(request(url, accessToken));

    long begin = DateTime.parseRfc3339(beginTime).getValue() * 1000000;
    long end = DateTime.parseRfc3339(endTime).getValue() * 1000000;

    datasources.load(request("https://www.googleapis.com/fitness/v1/users/me/dataSources", accessToken));

    for (JsonElement source : datasources.getDatasources()) {
      String dataStreamId = source.getAsJsonObject().get("dataStreamId").getAsString();
      LOG.debug(dataStreamId);

      url = String.format("https://www.googleapis.com/fitness/v1/users/me/dataSources/%s/datasets/%d-%d",
          URLEncoder.encode(dataStreamId, "utf-8"), begin, end);
      datasets.load(request(url, accessToken));
    }
   
    for (int i = 0; i < aggregatedDataTypeNames.length; ++i) {
//      aggregatedDatasetsModel.load(requestAggregate("https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate", aggregatedDataTypeNames[i], accessToken, begin, end, 86400000));
      aggregatedDatasetsModel.load(requestAggregate("https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate", aggregatedDataTypeNames[i], accessToken, begin, end, 3600000));
    }
  }

  private String request(String stringURL, String accessToken) throws IOException {
    LOG.info(stringURL);
    URL url = new URL(stringURL);
    HttpURLConnection uc = (HttpURLConnection) url.openConnection();
    uc.setRequestProperty("X-Requested-With", "Curl");
    uc.setRequestProperty("Accept", "application/json");
    uc.setRequestProperty("Content-Type", "application/json");
    uc.setRequestProperty("Authorization", String.format("Bearer %s", accessToken));

    InputStreamReader inputStreamReader;
    if (200 <= uc.getResponseCode() && uc.getResponseCode() <= 299) {
      inputStreamReader = new InputStreamReader(uc.getInputStream());
    } else {
      inputStreamReader = new InputStreamReader(uc.getErrorStream());
    }
    BufferedReader br = new BufferedReader(inputStreamReader);
    String temp;
    StringBuilder sb = new StringBuilder();
    while ((temp = br.readLine()) != null) {
      sb.append(temp);
      sb.append("\r\n");
    }
    return sb.toString();
  }
  
  private String requestAggregate(String stringURL, String dataTypeName, String accessToken, long begin, long end, long durationMillis) throws IOException {
    LOG.info(stringURL);
    Map<String,Object> aggregateBy = new LinkedHashMap<>();
    aggregateBy.put("dataTypeName", dataTypeName);
         
    List<Object> list = new LinkedList<Object>();
    list.add(aggregateBy);
    Map<String,Object> bucketByTime = new LinkedHashMap<>();
    bucketByTime.put("durationMillis", durationMillis);
    
    Map<String,Object> params = new LinkedHashMap<>();
    params.put("aggregateBy", list);
    params.put("bucketByTime", bucketByTime);
    params.put("startTimeMillis", begin / 1000000);
    params.put("endTimeMillis", end / 1000000);

    Gson gson = new Gson();
    String jsonPost = gson.toJson(params);
    byte[] postDataBytes = jsonPost.getBytes("UTF-8");

    URL url = new URL(stringURL);
    HttpURLConnection uc = (HttpURLConnection) url.openConnection();
    uc.setRequestMethod("POST");
    uc.setRequestProperty("X-Requested-With", "Curl");
    uc.setRequestProperty("Accept", "application/json");
    uc.setRequestProperty("Content-Type", "application/json;encoding=utf-8");
    uc.setRequestProperty("Authorization", String.format("Bearer %s", accessToken));
    uc.setDoOutput(true);
    uc.getOutputStream().write(postDataBytes);

    InputStreamReader inputStreamReader;
    if (200 <= uc.getResponseCode() && uc.getResponseCode() <= 299) {
      inputStreamReader = new InputStreamReader(uc.getInputStream());
    } else {
      inputStreamReader = new InputStreamReader(uc.getErrorStream());
    }
    BufferedReader br = new BufferedReader(inputStreamReader);
    String temp;
    StringBuilder sb = new StringBuilder();
    while ((temp = br.readLine()) != null) {
      sb.append(temp);
      sb.append("\r\n");
    }
    return sb.toString();
  }


}
