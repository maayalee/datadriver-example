package com.maayalee.dd.importers.loadrescutetime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RescuetimePresenter {
  private static final Logger LOG = LoggerFactory.getLogger(App.class);

  public RescuetimePresenter(AnalyticDataModel analyticData) {
    this.analyticData = analyticData;
  }

  public void load(String apiKey, String beginDate, String endDate) throws Exception {
    String url = String.format(
        "https://www.rescuetime.com/anapi/data?key=%s&restrict_begin=%s&restrict_end=%s&perspective=interval&resolution_time=hour",
        apiKey, beginDate, endDate);
    analyticData.load(requestHTTP(url));
  }

  private String requestHTTP(String stringURL) throws Exception {
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
      throw new Exception(String.format("error code %d, error message: s%", uc.getResponseCode(), uc.getResponseMessage()));
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
