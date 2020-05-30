package com.maayalee.dd.importers.loadgooglefitness;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.extensions.java6.auth.oauth2.GooglePromptReceiver;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.fitness.FitnessScopes;

public class App {
  private static final Logger LOG = LoggerFactory.getLogger(App.class);
  private static final String NAME = "Google fitness importer";
  private static final String VERSION = "1.0";

  // 유저 인증 정보를 저장할 디렉토리 위치 정보 지정
  private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"),
      ".store/dd_fitness");

  private static FileDataStoreFactory dataStoreFactory;
  private static HttpTransport httpTransport;
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  
  private static Set<String> permissions = new HashSet<String>();

  // 유저 정보에 접근하기 위한 인증 정보 얻기
  private static Credential authorize() throws Exception {
    // client secrets 정보를 로드한다.
    InputStream is = App.class.getResourceAsStream("/client_secrets.json");
    if (null == is) {
      // jar 파일로 실행시 파일 접근
      is = App.class.getResourceAsStream("/resources/client_secrets.json");
    }
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(is));
    // 구글피트니스 API에 대한 접근 권한 지정  
    permissions.add(FitnessScopes.FITNESS_ACTIVITY_READ);
    permissions.add(FitnessScopes.FITNESS_LOCATION_READ);
    permissions.add(FitnessScopes.FITNESS_REPRODUCTIVE_HEALTH_READ);
    permissions.add(FitnessScopes.FITNESS_BODY_READ);
    // 백그라운드에서 실행하기 위한 offline 타입의 인증 처리를 요청한다
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY,
        clientSecrets, permissions).setDataStoreFactory(dataStoreFactory).setAccessType("offline").build();
    // authorize
    //return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    return new AuthorizationCodeInstalledApp(flow, new GooglePromptReceiver()).authorize("user");
  }

  public static void main(String[] args) {
    Options options = new Options();
    
    Option userIdOption = new Option("", "user_id", true, "");
    userIdOption.setRequired(true);
    options.addOption(userIdOption);

    Option beginTimeOption = new Option("", "begin_time", true, "");
    beginTimeOption.setRequired(true);
    options.addOption(beginTimeOption);

    Option endTimeOption = new Option("", "end_time", true, "");
    endTimeOption.setRequired(true);
    options.addOption(endTimeOption);

    Option outputDirectoryOption = new Option("", "output_directory", true, "");
    outputDirectoryOption.setRequired(true);
    options.addOption(outputDirectoryOption);

    Option outputFilenamePrefixOption = new Option("", "output_filenameprefix", true, "");
    outputFilenamePrefixOption.setRequired(true);
    options.addOption(outputFilenamePrefixOption);

    Option shardSizeOption = new Option("s", "shard_size", true, "");
    shardSizeOption.setRequired(true);
    options.addOption(shardSizeOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      CommandLine cmd = parser.parse(options, args);

      String userId = cmd.getOptionValue("user_id");
      String beginTime = cmd.getOptionValue("begin_time");
      String endTime = cmd.getOptionValue("end_time");
      String shardSizeString = cmd.getOptionValue("shard_size").replaceAll("\"", "");
      int shardSize = Integer.parseInt(shardSizeString);
      String outputDirectory = cmd.getOptionValue("output_directory");
      String outputFilenamePrefix = cmd.getOptionValue("output_filenameprefix");

      LOG.info(NAME + " (Ver." + VERSION + ")");
      LOG.info(String.format("begin_time:%s, end_time:%s, shard_size:%s", beginTime, endTime, shardSizeString));
      LOG.info(String.format("output_directory:%s, output_filenameprefix:%s, shard_size:%s", outputDirectory,
          outputFilenamePrefix, shardSizeString));

      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
      Credential credential = authorize();
      // 인증 정보의 유효 시간이 지나는 경우 자동으로 연장 한다.
      if (credential.getExpiresInSeconds() < 0) {
        LOG.info("Refresh accesstoken");
        credential.refreshToken();
      }
      LOG.info(credential.getAccessToken());

      DatasourcesModel datasources = new DatasourcesModel(userId);
      DatasetsModel datasets = new DatasetsModel(userId);
      AggregatedDatasetsModel aggregatedDatasets = new AggregatedDatasetsModel(userId);
      SessionsModel sessions = new SessionsModel(userId);
      GoogleFitnessPresenter presetner = new GoogleFitnessPresenter(datasources, datasets, aggregatedDatasets, sessions);
      presetner.load(beginTime, endTime, credential.getAccessToken());

      String[] tokens = (outputDirectory + "/" + outputFilenamePrefix).replaceAll("gs://", "").split("/");
      String bucketName = tokens[0];
      String prefix = "";
      for (int i = 1; i < tokens.length; ++i) {
        prefix += tokens[i];
        if (i != (tokens.length - 1)) {
          prefix += "/";
        }
      }
      LOG.info("bucketName:" + bucketName + ", prefix: " + prefix);
      GCSModel gcs = new GCSModel();
      gcs.delete(bucketName, prefix);
      gcs.write(bucketName, prefix + "datasources-", datasources.getDatasources(), shardSize);
      gcs.write(bucketName, prefix + "sessions-", sessions.getSessions(), shardSize);
      gcs.write(bucketName, prefix + "datasets-", datasets.getDatasets(), shardSize);
      gcs.write(bucketName, prefix + "aggregated-datasets-", aggregatedDatasets.getDatasets(), shardSize);

    } catch (ParseException e) {
      LOG.info(e.getMessage());
      formatter.printHelp(NAME, options);
      System.exit(1);
    } catch (Exception e) {
      LOG.error("Excpetion occured: " + getStackTrace(e));
      System.exit(1);
    }
    System.exit(0);
  }

  private static String getStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
