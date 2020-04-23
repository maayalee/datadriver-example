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

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.DateTime;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

// HTTP요청시 DATA STORE DIR의 파일을 다른 프로그램과 공유하면 scope가 허용안된 인증 확인 정보 문제 때문에 Insufficent Permission 에러 응답을 받을 수 있다.
public class GoogleFitnessPresenter {
  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  public GoogleFitnessPresenter(DatasourcesModel datasources, DatasetsModel datasets, SessionsModel sessions) {
    this.datasources = datasources;
    this.datasets = datasets;
    this.sessions = sessions;
  }

  public void load(String beginTime, String endTime, String accessToken) throws IOException {
    String url = String.format("https://www.googleapis.com/fitness/v1/users/me/sessions?startTime=%s&endTime=%s",
        URLEncoder.encode(beginTime, "utf-8"), URLEncoder.encode(endTime, "utf-8"));
    sessions.load(requestHTTP(url, accessToken));

    long begin = DateTime.parseRfc3339(beginTime).getValue() * 1000000;
    long end = DateTime.parseRfc3339(endTime).getValue() * 1000000;

    datasources.load(requestHTTP("https://www.googleapis.com/fitness/v1/users/me/dataSources", accessToken));

    for (JsonElement source : datasources.getDatasources()) {
      String dataStreamId = source.getAsJsonObject().get("dataStreamId").getAsString();
      LOG.info(dataStreamId);

      url = String.format("https://www.googleapis.com/fitness/v1/users/me/dataSources/%s/datasets/%d-%d",
          URLEncoder.encode(dataStreamId, "utf-8"), begin, end);
      datasets.load(requestHTTP(url, accessToken));
    }
    
    url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate";
    LOG.info(requestAggregateTest(url, accessToken, begin, end));
  }

  private String requestHTTP(String stringURL, String accessToken) throws IOException {
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
  
  private String requestAggregateTest(String stringURL, String accessToken, long begin, long end) throws IOException {
    Map<String,Object> aggregateBy = new LinkedHashMap<>();
    aggregateBy.put("dataTypeName", "com.google.step_count.delta");
    aggregateBy.put("dataSourceId", "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps");
    List<Object> list = new LinkedList<Object>();
    list.add(aggregateBy);
    Map<String,Object> bucketByTime = new LinkedHashMap<>();
    bucketByTime.put("durationMillis", 86400000);
    
    Map<String,Object> params = new LinkedHashMap<>();
    params.put("aggregateBy", list);
    params.put("bucketByTime", bucketByTime);
    params.put("startTimeMillis", begin / 1000000);
    params.put("endTimeMillis", end / 1000000);

    Gson gson = new Gson();
    String jsonPost = gson.toJson(params);
    LOG.info(jsonPost);
    byte[] postDataBytes = jsonPost.getBytes("UTF-8");

    
    LOG.info(stringURL);
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

  private DatasourcesModel datasources;
  private DatasetsModel datasets;
  private SessionsModel sessions;
}
