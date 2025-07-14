"""
TaskFlow API를 사용한 DAG 생성 방식 (Modern Approach)
- @dag, @task 데코레이터 사용
- 자동 XCom 처리 및 타입 힌팅 지원
- 더 간결하고 Pythonic한 코드
- 데이터 의존성이 자동으로 관리됨
"""

from airflow.decorators import dag, task
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


@dag(
    dag_id='example_simple_dag2',  # 명시적 DAG ID 설정
    default_args=default_args,
    description='TaskFlow API를 사용한 간단한 데이터 처리 DAG',
    schedule='@daily',
    catchup=False,
    tags=['example', 'taskflow', 'modern', 'beginner'],
)
def taskflow_example_dag():
    """TaskFlow API를 사용한 데이터 처리 파이프라인"""

    # 시작 태스크 (BashOperator 사용)
    start = BashOperator(
        task_id='start',
        bash_command='echo "TaskFlow DAG 시작!"'
    )

    @task
    def extract_data() -> str:
        """데이터 추출 - 반환값이 자동으로 XCom에 저장됨"""
        print("데이터를 추출하는 중...")
        return "추출된 데이터"

    @task
    def transform_data(raw_data: str) -> str:
        """데이터 변환 - 파라미터로 이전 태스크 결과를 자동 수신"""
        print(f"데이터를 변환하는 중... 입력: {raw_data}")
        return f"변환된_{raw_data}"

    @task
    def load_data(processed_data: str) -> None:
        """데이터 적재 - 타입 힌팅으로 명확한 인터페이스"""
        print(f"데이터를 적재하는 중... 데이터: {processed_data}")
        print("데이터 파이프라인 완료!")

    @task
    def send_notification(processed_data: str) -> None:
        """완료 알림 - 병렬 처리 예시"""
        print(f"처리 완료 알림 발송: {processed_data}")

    # 종료 태스크 (BashOperator 사용)
    end = BashOperator(
        task_id='end',
        bash_command='echo "TaskFlow DAG 완료!"'
    )

    # TaskFlow API의 자동 의존성 관리
    # 함수 호출과 데이터 흐름이 의존성을 정의함
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)

    # 의존성 설정 (데이터 흐름 + 수동 의존성)
    start >> extracted_data
    transformed_data >> [
        load_data(transformed_data), send_notification(transformed_data)] >> end


# DAG 인스턴스 생성
taskflow_example_dag()
