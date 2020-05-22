package com.maayalee.dd.etls.createrescuetimebd;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {
    try {
      CreateJsonBDOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
          .as(CreateJsonBDOptions.class);
      LOG.info("appname:" + options.getAppName());

      CreateJsonBDRunner.start(options);
    } catch (NullPointerException e) {
      LOG.error("NullPointerException occured: " + getStackTrace(e));
    } catch (Exception e) {
      LOG.error("Excpetion occured: " + getStackTrace(e));
    }
  }
  
  private static String getStackTrace(Exception e) {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
