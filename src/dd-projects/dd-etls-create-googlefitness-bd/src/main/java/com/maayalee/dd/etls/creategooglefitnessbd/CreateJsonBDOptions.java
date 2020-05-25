package com.maayalee.dd.etls.creategooglefitnessbd;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface CreateJsonBDOptions extends DataflowPipelineOptions {
  @Description("Begin range time")
  ValueProvider<String> getBeginTime();
  void setBeginTime(ValueProvider<String> value);
  
 @Description("End range time")
  ValueProvider<String> getEndTime();
  void setEndTime(ValueProvider<String> value);

  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputDatasetsFilePattern();
  void setInputDatasetsFilePattern(ValueProvider<String> value);
  
  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputAggregatedDatasetsFilePattern();
  void setInputAggregatedDatasetsFilePattern(ValueProvider<String> value);

 @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputSessionsFilePattern();
  void setInputSessionsFilePattern(ValueProvider<String> value);
  
  @Description("Output BigQuery table schema json gs:// path")
  ValueProvider<String> getTableSchemaDatasetsJSONPath();
  void setTableSchemaDatasetsJSONPath(ValueProvider<String> value);
  
  @Description("Output BigQuery table schema json gs:// path")
  ValueProvider<String> getTableSchemaAggregatedDatasetsJSONPath();
  void setTableSchemaAggregatedDatasetsJSONPath(ValueProvider<String> value);

  @Description("Output BigQuery table schema json gs:// path")
  ValueProvider<String> getTableSchemaSessionsJSONPath();
  void setTableSchemaSessionsJSONPath(ValueProvider<String> value);

  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputDatasetsTable();
  void setOutputDatasetsTable(ValueProvider<String> value);
  
  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputAggregatedDatasetsTable();
  void setOutputAggregatedDatasetsTable(ValueProvider<String> value);
  
  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputSessionsTable();
  void setOutputSessionsTable(ValueProvider<String> value); 
}