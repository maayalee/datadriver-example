package com.maayalee.dd.importers.loadgooglefitness;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;

public class GCSModel {
  private static final Logger LOG = LoggerFactory.getLogger(GCSModel.class);
  
  private Storage storage;

  public GCSModel() throws IOException {
    //storage = StorageOptions.getDefaultInstance().getService();
    storage = StorageOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream(getKey())).
        build().getService();
  }

  private InputStream getKey() throws IOException {
    InputStream is = App.class.getResourceAsStream("/service_account.json");
    if (null == is) {
      // jar 파일로 실행시 파일 접근
      is = App.class.getResourceAsStream("/resources/service_account.json");
    }
    return is;
  }

  public void delete(String bucketName, String prefix) {
    Iterable<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix)).iterateAll();
    for (Blob blob : blobs) {
      LOG.info(blob.delete(Blob.BlobSourceOption.generationMatch()) ? "Deleted:" + blob.getName()
          : "Not deleted:" + blob.getName());
    }
  }

  public void write(String bucketName, String prefix, JsonArray rows, int shardSize) {
    LOG.info("bucketName: " + bucketName);
    LOG.info("prefix: " + prefix);
    StringBuilder[] outputs = new StringBuilder[shardSize];
    for (int i = 0; i < shardSize; ++i) {
      outputs[i] = new StringBuilder("");
    }
    Gson gson = new Gson();
    for (int i = 0; i < rows.size(); ++i) {
      String rowString = gson.toJson(rows.get(i));
      int shardIndex = (i % shardSize);
      outputs[shardIndex].append(rowString);
      outputs[shardIndex].append("\r\n");
    }

    for (int i = 0; i < shardSize; ++i) {
      String fileName = prefix + String.format("%d-of-%d.jsonl", i, shardSize);
      LOG.info("filename: " + fileName);
      LOG.info("output: " + outputs[i].toString());
      storage.create(BlobInfo.newBuilder(bucketName, fileName).setContentType("text/html").build(),
          outputs[i].toString().getBytes());
    }
  }
}
