package com.maayalee.dd.etls.creategooglefitnessbd;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface CreateJsonBDOptions extends DataflowPipelineOptions {
  @Description("Begin range time")
  ValueProvider<String> getBeginTime();
  void setBeginTime(ValueProvider<String> value);
  
 @Description("End range time")
  ValueProvider<String> getEndTime();
  void setEndTime(ValueProvider<String> value);

  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputAggregatedDatasetsFilePattern();
  void setInputAggregatedDatasetsFilePattern(ValueProvider<String> value);

  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputSessionsFilePattern();
  void setInputSessionsFilePattern(ValueProvider<String> value);
   
  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputAggregatedDatasetsTable();
  void setOutputAggregatedDatasetsTable(ValueProvider<String> value);
  
  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputSessionsTable();
  void setOutputSessionsTable(ValueProvider<String> value);
  
  @Description("Output BigQuery table schema json gs:// path")
  String getTableSchemaAggregatedDatasetsJSONPath();
  void setTableSchemaAggregatedDatasetsJSONPath(String value);

  @Description("Output BigQuery table schema json gs:// path")
  String getTableSchemaSessionsJSONPath();
  void setTableSchemaSessionsJSONPath(String value);
  
  String getClusteringField();
  void setClusteringField(String value);
}