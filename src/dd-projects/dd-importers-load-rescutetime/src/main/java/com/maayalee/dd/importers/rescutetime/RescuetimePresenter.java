package com.maayalee.dd.importers.rescutetime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RescuetimePresenter {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public RescuetimePresenter(AnalyticDataModel analyticData) {
    this.analyticData = analyticData;
  }

  public void load(String apiKey, String beginDate, String endDate) throws IOException {
    String url = String.format(
        "https://www.rescuetime.com/anapi/data?key=%s&restrict_begin=%s&restrict_end=%s&perspective=interval&resolution_time=hour",
        apiKey, beginDate, endDate);
    analyticData.load(requestHTTP(url));
  }

  private String requestHTTP(String stringURL) throws IOException {
    LOG.info(stringURL);
    URL url = new URL(stringURL);
    HttpURLConnection uc = (HttpURLConnection) url.openConnection();
    uc.setRequestProperty("X-Requested-With", "Curl");
    uc.setRequestProperty("Accept", "application/json");
    uc.setRequestProperty("Content-Type", "application/json");

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

  private AnalyticDataModel analyticData;
}
