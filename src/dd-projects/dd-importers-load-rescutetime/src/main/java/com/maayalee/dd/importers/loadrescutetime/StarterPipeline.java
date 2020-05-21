/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.maayalee.dd.importers.loadrescutetime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  private static final String NAME = "Rescute time importer";
  private static final String VERSION = "1.0";;

  public static void main(String[] args) {
    Options options = new Options();

    Option apiKeyOption = new Option("", "api_key", true, "");
    apiKeyOption.setRequired(true);
    options.addOption(apiKeyOption);

    Option beginDateOption = new Option("", "begin_date", true, "");
    beginDateOption.setRequired(true);
    options.addOption(beginDateOption);

    Option endDateOption = new Option("", "end_date", true, "");
    endDateOption.setRequired(true);
    options.addOption(endDateOption);

    Option outputDirectoryOption = new Option("", "output_directory", true, "");
    outputDirectoryOption.setRequired(true);
    options.addOption(outputDirectoryOption);

    Option outputFilenamePrefixOption = new Option("", "output_filenameprefix", true, "");
    outputFilenamePrefixOption.setRequired(true);
    options.addOption(outputFilenamePrefixOption);

    Option shardSizeOption = new Option("", "shard_size", true, "");
    shardSizeOption.setRequired(true);
    options.addOption(shardSizeOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    try {
      CommandLine cmd = parser.parse(options, args);

      String apiKey = cmd.getOptionValue("api_key");
      String beginDate = cmd.getOptionValue("begin_date");
      String endDate = cmd.getOptionValue("end_date");
      int shardSize = Integer.parseInt(cmd.getOptionValue("shard_size"));
      String outputDirectory = cmd.getOptionValue("output_directory");
      String outputFilenamePrefix = cmd.getOptionValue("output_filenameprefix");

      System.out.println(NAME + " (Ver." + VERSION + ")");
      System.out.println(String.format("key:%s, begin_date:%s, end_date:%s", apiKey, beginDate, endDate));

      AnalyticDataModel analyticDataModel = new AnalyticDataModel();
      RescuetimePresenter rescuetimePresneter = new RescuetimePresenter(analyticDataModel);
      rescuetimePresneter.load(apiKey, beginDate, endDate);

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
      GCSModel gcsModel = new GCSModel();
      gcsModel.delete(bucketName, prefix);
      gcsModel.write(bucketName, prefix, analyticDataModel.getAnalyticDatas(), shardSize);
    } catch (ParseException e) {
      LOG.info(e.getMessage());
      formatter.printHelp(NAME, options);
      System.exit(1);
    } catch (NullPointerException e) {
      LOG.error("NullPointerException occured: " + getStackTrace(e));
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
