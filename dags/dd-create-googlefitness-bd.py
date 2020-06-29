import airflow
from airflow.models import DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataflow_operator
from datetime import date, timedelta, datetime
from pytz import timezone

project_id = 'fast-archive-274910'
dataflow_gs = 'gs://datadriver-dataflow-fast-archive-274910'
datalake_gs = 'gs://datadriver-datalake-fast-archive-274910'
user_id = 'maayalee'

default_dag_args = {
        'start_date': datetime(2018, 7, 1),
        'dataflow_default_options': {
            'project': project_id,
            'region':'asia-northeast1',
            'tempLocation':'{}/tmp'.format(dataflow_gs)
         },
        'retries': 3,
        'retry_delay': timedelta(minutes=30) 
}

dag = DAG(
        'dd-create-googlefitness-bd-v1',
        schedule_interval='0 0 * * *',
        default_args=default_dag_args)

begin_times = ['{{ macros.ds_add(ds, -1) }}T00:00:00.00Z', '{{ macros.ds_add(ds, 0) }}T00:00:00.00Z']
end_times = ['{{ macros.ds_add(ds, -1) }}T23:59:59.99Z', '{{ macros.ds_add(ds, 0) }}T23:59:59.99Z']
filename_prefixes = [
  '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}Z-' + user_id + '-', 
  '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}Z-' + user_id + '-'
]
bd_dates = [
  '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d") }}', 
  '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}'
]

# 한국시 기준 데이터로 보여주기 위해 UTC 기준2일치 데이터를 처리
for i in range(2):
  output_directory = '{}/data/log/googlefitness'.format(datalake_gs)
  load_googlefitness = bash_operator.BashOperator(
          task_id=('load_googlefitness-%s' % i),
          bash_command='java -jar ${{AIRFLOW_HOME}}/dags/dd-importers-load-googlefitness.jar -user_id={} -begin_time={} -end_time={} -output_directory={}  -output_filenameprefix={} -shard_size=3'.format(user_id, begin_times[i], end_times[i], output_directory, filename_prefixes[i]),
          dag=dag)

  create_googlefitness_bd = dataflow_operator.DataflowTemplateOperator(
          task_id=('create_googlefitness_bd-%s' % i),
          template='{}/templates/dd-etls-create-googlefitness'.format(dataflow_gs),
          parameters={
            'runner':'DataflowRunner',
            'beginTime':begin_times[i],
            'endTime':end_times[i],
            'inputAggregatedDatasetsFilePattern':'{}/data/log/googlefitness/{}Z-*-aggregated-datasets-*'.format(datalake_gs, bd_dates[i]),
            'inputSessionsFilePattern':'{}/data/log/googlefitness/{}Z-*-sessions-*'.format(datalake_gs, bd_dates[i]),
            'outputAggregatedDatasetsTable':'{}:dw_datadriver.googlefitness_tbl_bd_aggregated_datasets${}'.format(project_id, bd_dates[i]),
            'outputSessionsTable':'{}:dw_datadriver.googlefitness_tbl_bd_sessions${}'.format(project_id, bd_dates[i])
          },
          dag=dag,
          gcp_conn_id='gcp-airflow-service-account'
  )
  create_googlefitness_bd.set_upstream(load_googlefitness);
