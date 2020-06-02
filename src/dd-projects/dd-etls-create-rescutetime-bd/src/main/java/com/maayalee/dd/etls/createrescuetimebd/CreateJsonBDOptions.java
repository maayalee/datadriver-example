package com.maayalee.dd.etls.createrescuetimebd;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface CreateJsonBDOptions extends DataflowPipelineOptions {
  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputFilePattern();
  void setInputFilePattern(ValueProvider<String> value);
  
  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputTable();
  void setOutputTable(ValueProvider<String> value);
  
  @Description("Output BigQuery table schema json gs:// path")
  String getTableSchemaJSONPath();
  void setTableSchemaJSONPath(String value);
  
  String getClusteringField();
  void setClusteringField(String value);
}