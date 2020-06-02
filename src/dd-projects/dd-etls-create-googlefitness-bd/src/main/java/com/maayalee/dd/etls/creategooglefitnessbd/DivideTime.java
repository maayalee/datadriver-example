package com.maayalee.dd.etls.creategooglefitnessbd;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.client.util.DateTime;

@SuppressWarnings("serial")
class DivideTime extends DoFn<TableRow, TableRow> {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  private static final Long THIRTY_MINUTES = 1800000L;

  public DivideTime(ValueProvider<String> beginTime, ValueProvider<String> endTime) {
    this.beginTime = beginTime;
    this.endTime = endTime;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    TableRow tableRow = c.element();
    try {
      TableRow newTableRow = tableRow.clone();
      Long beginTimeOfDate = DateTime.parseRfc3339(beginTime.get()).getValue();
      Long endTimeOfDate = DateTime.parseRfc3339(endTime.get()).getValue();
      Long startTime = Long.parseLong((String)tableRow.get("startTimeMillis"));
      Long endTime = Long.parseLong((String)tableRow.get("endTimeMillis"));

      // 이틀에 걸쳐 중복되는 데이터는 범위에 해당하는 날짜마다 데이터를 돌려준다. 따라서 로드하는 날짜의 범위안에 있는 시간으로 변경해준다. 
      if (startTime < beginTimeOfDate) {
        newTableRow.set("startTimeMillis", beginTimeOfDate);
        startTime = beginTimeOfDate;
      }
      if (endTime > endTimeOfDate) {
        newTableRow.set("endTimeMillis", endTimeOfDate);
        endTime = endTimeOfDate;
      }
      // 다양한 타임존 기준으로 측정을 위해 데이터의 범위를 30분 단위로 분리해서 저장한다. 
      if ((endTime - startTime) > THIRTY_MINUTES) {
        Long currentStartTime = startTime;
        Long currentEndTime = startTime + THIRTY_MINUTES;
        while (true) {
          TableRow divideTableRow = newTableRow.clone();
          divideTableRow.set("startTimeMillis", currentStartTime);
          divideTableRow.set("endTimeMillis", currentEndTime > endTime ? endTime : currentEndTime);
          c.output(divideTableRow);

          if (currentEndTime > endTime) {
            break;
          }
          currentStartTime += THIRTY_MINUTES;
          currentEndTime += THIRTY_MINUTES;
        }
      }
      else {
        c.output(newTableRow);
      }

    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      LOG.error(String.format("Occured exception: %s", sw.toString()));
    }
  }

  private ValueProvider<String> beginTime;
  private ValueProvider<String> endTime;
}
