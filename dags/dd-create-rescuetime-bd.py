from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataflow_operator
from datetime import date, timedelta, datetime
from pytz import timezone

default_dag_args = {
        'start_date': datetime(2020, 5, 21),
        'dataflow_default_options': {
            'project': 'fast-archive-274910',
            'region':'asia-northeast1',
            'tempLocation':'gs://datadriver-dataflow-fast-archive-274910/tmp'
         },
        'retries': 3,
        'retry_delay': timedelta(minutes=30) 
}

dag = DAG(
        'dd-create-rescuetime-bd-v1',
        schedule_interval='0 16 * * *',
        default_args=default_dag_args)

api_key = 'B63lJwmLgMWhDcvVf9nmVUtwtVVagWPrZmgFiBF9'
input_begin_dates = ['{{ macros.ds_add(ds, 0) }}', '{{ macros.ds_add(ds, 1) }}']
input_end_dates = ['{{ macros.ds_add(ds, 1) }}', '{{ macros.ds_add(ds, 2) }}']
output_filename_prefixes = [
  '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}Z-maayalee-', 
  '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}Z-maayalee-'
]
bd_dates = [
  '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}', 
  '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'
]

# 한국시 기준 데이터로 보여주기 위해 UTC 기준2일치 데이터를 처리
for i in range(2):
  output_directory = 'gs://datadriver-datalake-fast-archive-274910/data/log/rescuetime'
  load_rescuetime = bash_operator.BashOperator(
          task_id=('load_rescuetime-%s' % i),
          bash_command='java -jar ${{AIRFLOW_HOME}}/dags/dd-importers-load-rescuetime.jar -user_id=maayalee -api_key={} -input_begin_date={} -input_end_date={} -input_timezone=Asia/Seoul -output_date={} -output_timezone=UTC -output_directory={}  -output_filenameprefix={} -shard_size=3'.format(api_key, input_begin_dates[i], input_end_dates[i], input_begin_dates[i], output_directory, output_filename_prefixes[i]),
          dag=dag)

  create_rescuetime_bd = dataflow_operator.DataflowTemplateOperator(
          task_id=('create_rescuetime_bd-%s' % i),
          template='gs://datadriver-dataflow-fast-archive-274910/templates/dd-etls-create-rescuetime',
          parameters={
            'runner':'DataflowRunner',
            'inputFilePattern':'gs://datadriver-datalake-fast-archive-274910/data/log/rescuetime/{}Z-*'.format(bd_dates[i]),
            'outputTable':'fast-archive-274910:dw_datadriver.rescuetime_tbl_bd_data${}'.format(bd_dates[i])
          },
          dag=dag,
          gcp_conn_id='gcp-airflow-service-account'
  )
  create_rescuetime_bd.set_upstream(load_rescuetime);
