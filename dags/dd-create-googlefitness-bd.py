from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataflow_operator
from datetime import date, timedelta, datetime
from pytz import timezone

default_dag_args = {
        'start_date': datetime(2018, 7, 1),
        'dataflow_default_options': {
            'project': 'rising-field-212511',
            'zone': 'asia-northeast1-c',
            'tempLocation': 'gs://datadriver-datalake/temp'
            },
        'retries': 3,
        'retry_delay': timedelta(minutes=30)
        }

dag = DAG(
        'dd-create-googlefitness-bd-v4',
        schedule_interval='30 0 * * *',
        default_args=default_dag_args)

begin_time = '{{ macros.ds_add(ds, 0) }}T00:00:00.00+09:00'
end_time = '{{ macros.ds_add(ds, 0) }}T23:59:59.99+09:00'
filename_prefix = '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}+9:00-' 
load_googlefitness = bash_operator.BashOperator(
        task_id='load_googlefitness',
        bash_command='java -jar ~/airflow/dags/dd-importers-load-googlefitness.jar -begin_time={} -end_time={} -output_directory=gs://datadriver-datalake/data/log/googlefitness  -output_filenameprefix={} -shard_size=3'.format(begin_time, end_time, filename_prefix),
        dag=dag)



bd_date = '{{ macros.ds_format(macros.ds_add(ds, 0), "%Y-%m-%d", "%Y%m%d") }}'
create_googlefitness_datasets = bash_operator.BashOperator(
        task_id='create_googlefitness_datasets',
        bash_command='java -jar ~/airflow/dags/dd-analyzers-create-json-bd.jar --runner=DirectRunner --project=rising-field-212511 --tempLocation=gs://datadriver-dataflow/temp --stagingLocation=gs://datadriver-dataflow/temp --inputFilePattern=gs://datadriver-datalake/data/log/googlefitness/{}+9:00-datasets-* --timezone=Asia/Seoul --tableSchemaJSONPath=gs://datadriver-dataflow/rules/googlefitness_schemas_datasets.json --outputTable=rising-field-212511:dw_datadriver.googlefitness_tbl_bd_datasets\${}'.format(bd_date, bd_date),
        dag=dag
        )
create_googlefitness_datasets.set_upstream(load_googlefitness);
