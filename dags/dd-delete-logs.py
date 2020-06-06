import airflow
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import date, timedelta

default_dag_args = {
        "start_date": airflow.utils.dates.days_ago(1),
        'retries': 3, # 작업 실패시 재시도 횟수
        'retry_delay': timedelta(minutes=30) # 작업 재시도시 지연 시간
}

dag = models.DAG(
        'dd-delete-logs-v1', # DAG id
        schedule_interval='0 0 * * *', # 크론탭 처럼 실행 간격을 예약. 매일 0시에 실행
        default_args=default_dag_args # 파라미터 설정
)

echo_task1 = BashOperator(
        task_id='delete_log_task',
        bash_command='sudo rm -rf `find ${AIRFLOW_HOME}/logs -maxdepth 1 -name \'dd*\' | awk \'{print $1}\'`', # dd로 시작하는 폴더 삭제
        dag=dag
)
