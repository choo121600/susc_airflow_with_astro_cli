"""
전통적인 DAG 생성 방식 (Classic Approach)
- DAG 객체를 직접 생성
- 각 태스크를 개별 오퍼레이터로 생성
- 명시적인 의존성 설정 필요
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 기본 설정 정의
default_args = {
    'owner': 'airflow_student',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 인스턴스 생성 (전통적인 방식)
dag = DAG(
    'example_simple_dag',  # DAG ID
    default_args=default_args,
    description='전통적인 방식으로 만든 간단한 데이터 처리 DAG',
    schedule='@daily',
    catchup=False,  # 과거 날짜에 대한 백필 비활성화
    tags=['example', 'traditional', 'beginner'],
)

# 함수 정의
def extract_data(**context):
    """데이터 추출 시뮬레이션"""
    print(f"데이터를 추출하는 중... (실행 날짜: {context['ds']})")
    return "추출된 데이터"


def transform_data(**context):
    """데이터 변환 시뮬레이션"""
    print("데이터를 변환하는 중...")
    # XCom에서 이전 태스크의 결과 가져오기
    extracted_data = context['task_instance'].xcom_pull(task_ids='extract')
    print(f"받은 데이터: {extracted_data}")
    return "변환된 데이터"


def load_data(**context):
    """데이터 적재 시뮬레이션"""
    print("데이터를 적재하는 중...")
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform')
    print(f"적재할 데이터: {transformed_data}")
    print("데이터 파이프라인 완료!")


# 태스크 생성 (전통적인 방식 - 각각 개별 오퍼레이터)
start_task = BashOperator(
    task_id='start',
    bash_command='echo "전통적인 DAG 시작!"',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

end_task = BashOperator(
    task_id='end',
    bash_command='echo "전통적인 DAG 완료!"',
    dag=dag,
)

# 태스크 간의 의존성 설정 (전통적인 방식)
start_task >> extract_task >> transform_task >> load_task >> end_task
