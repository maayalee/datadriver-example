package com.maayalee.dd.etls.createjsonbd;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface CreateJsonBDOptions extends DataflowPipelineOptions {
  @Description("Input cloud storage file pattern")
  ValueProvider<String> getInputFilePattern();
  void setInputFilePattern(ValueProvider<String> value);

  @Description("Load timezone")
  @Default.String("UTC")
  ValueProvider<String> getTimezone();
  void setTimezone(ValueProvider<String> value);
  
  @Description("Output BigQuery table schema json gs:// path")
  ValueProvider<String> getTableSchemaJSONPath();
  void setTableSchemaJSONPath(ValueProvider<String> value);

  @Description("Output BigQuery table spec")
  ValueProvider<String> getOutputTable();
  void setOutputTable(ValueProvider<String> value);
}