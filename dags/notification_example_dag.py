from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.models import Variable

# SMTP 알림 설정
# https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/_api/airflow/providers/smtp/notifications/smtp/index.html

def get_smtp_notification():
    from_email = Variable.get('NOTIFICATION_FROM_EMAIL')  # 발신자 이메일
    to_email = Variable.get('NOTIFICATION_TO_EMAIL')      # 수신자 이메일
    
    return send_smtp_notification(
        from_email=from_email,
        to=to_email,
        smtp_conn_id="smtp_default",
        subject="[Airflow 알림] {{ dag.dag_id }} - {{ ti.task_id }} 실패",
        html_content="""
        <h3>Airflow 태스크 실패 알림</h3>
        <p><strong>DAG ID:</strong> {{ dag.dag_id }}</p>
        <p><strong>Task ID:</strong> {{ ti.task_id }}</p>
        <p><strong>실행 시간:</strong> {{ ts }}</p>
        <p><strong>상태:</strong> 실패</p>
        """
    )

@dag(
    dag_id="email_notifications",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "notifications"],
    doc_md="""
    ## 이메일 알림 테스트 DAG
    
    이 DAG는 Airflow의 최신 SMTP 알림 기능을 테스트합니다.
    - 성공 태스크: 정상적으로 완료됩니다
    - 실패 태스크: 의도적으로 실패하여 이메일 알림을 트리거합니다
    """,
)
def email_notifications():
    """이메일 알림 테스트 DAG"""

    @task
    def success_task():
        """성공 테스트 태스크"""
        print("Success task completed!")
        return "success"

    @task(on_failure_callback=[get_smtp_notification()])
    def fail_task():
        """실패 테스트 태스크 (의도적 실패)"""
        raise Exception("Task failed intentionally for testing purpose")

    # Task dependencies 설정
    success_task() >> fail_task()


email_notifications()
