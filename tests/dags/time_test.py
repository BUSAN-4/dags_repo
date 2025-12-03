from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pendulum

# 한국 시간(KST) 기준 설정을 위해 타임존 지정
local_tz = pendulum.timezone("Asia/Seoul")

# ✅ 서비스 주소 (아까 확인한 것)
FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

def submit_flink_sql(**context):
    # 실행된 시간(Execution Date)을 로그에 남겨서 스케줄링 확인
    exec_date = context['execution_date']
    print(f"🚀 스케줄링 실행 시간(UTC): {exec_date}")
    print(f"Connecting to Flink Gateway at: {FLINK_GATEWAY_URL}")
    
    # 세션 생성
    session_url = f"{FLINK_GATEWAY_URL}/v1/sessions"
    headers = {"Content-Type": "application/json"}
    resp = requests.post(session_url, json={"sessionName": "scheduler_test"}, headers=headers)
    
    if resp.status_code != 200:
        print(f"Session creation failed: {resp.text}")
        return

    session_handle = resp.json()['sessionHandle']
    print(f"✅ Session Created: {session_handle}")

    # SQL 실행
    sql = "SELECT 'Scheduler Test Success'"
    statement_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
    resp = requests.post(statement_url, json={"statement": sql}, headers=headers)
    
    if resp.status_code == 200:
        op_handle = resp.json()['operationHandle']
        print(f"✅ SQL Submitted. Handle: {op_handle}")
    else:
        print(f"SQL Submit Failed: {resp.text}")

with DAG(
    'flink_schedule_test_5min',
    # ✅ 현재 시간보다 조금 과거로 start_date를 잡아야 바로 스케줄링이 시작됩니다.
    start_date=datetime(2025, 12, 3, 19, 30, tzinfo=local_tz), 
    # ✅ 5분마다 실행 (Cron 표현식: "*/5 * * * *")
    # 또는 timedelta(minutes=5) 사용 가능
    schedule="*/5 * * * *", 
    catchup=False, # 과거 거는 실행 안 함
    tags=['test', 'schedule'],
) as dag:

    run_task = PythonOperator(
        task_id='run_every_5_min',
        python_callable=submit_flink_sql,
        provide_context=True
    )