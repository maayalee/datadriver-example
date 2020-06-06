import airflow
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import date, timedelta

# DAG 파라티터 정의
default_dag_args = {
        'start_date': airflow.utils.dates.days_ago(2), # 에어플로우에 등록한 이틀 전부터 실행(UTC 타임 기준)
        'retries': 3, # 작업 실패시 재시도 횟수
        'retry_delay': timedelta(minutes=30) # 작업 재시도시 지연 시간
}

# DAG 인스턴스 생성
dag = models.DAG(
        'dd-hello-dag-v1', # DAG id
        schedule_interval='0 0 * * *', # 크론탭 처럼 실행 간격을 예약. 매일 0시에 실행
        default_args=default_dag_args # 파라미터 설정
)

# BashOperator를 이용한 작업 객체 정의  
echo_task1 = BashOperator(
        task_id='hello_task',
        bash_command='echo hello dag!', # echo 커맨드를 실행
        dag=dag
)

# BashOperator를 이용한 작업 객체 정의
echo_task2 = BashOperator(
        task_id='complete_task',
        bash_command='echo task complete', # echo 커맨드를 실행
        dag=dag
)

# echo_task1을 처리후 echo_task2를 실행하도록 흐름을 설정
echo_task2.set_upstream(echo_task1);
