from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

# ✅ 서비스 주소 (환경에 맞게 수정되어 있음)
FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

def submit_flink_sql(**context):
    # Airflow 3.0 대응: execution_date 대신 logical_date 사용
    exec_date = context.get('logical_date')
    print(f"🚀 스케줄링 실행 시간(UTC): {exec_date}")
    print(f"Connecting to Flink Gateway at: {FLINK_GATEWAY_URL}")
    
    # 1. 세션 생성
    session_url = f"{FLINK_GATEWAY_URL}/v1/sessions"
    headers = {"Content-Type": "application/json"}
    # 세션 이름에 실행 시간을 붙여서 구분하기 쉽게 함
    session_name = f"scheduler_test_{exec_date}"
    
    resp = requests.post(session_url, json={"sessionName": session_name}, headers=headers)
    
    if resp.status_code != 200:
        raise Exception(f"Session creation failed: {resp.text}")

    session_handle = resp.json()['sessionHandle']
    print(f"✅ Session Created: {session_handle}")

    # 2. SQL 실행
    sql = "SELECT 'Scheduler Test Success'"
    statement_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
    resp = requests.post(statement_url, json={"statement": sql}, headers=headers)
    
    if resp.status_code == 200:
        op_handle = resp.json()['operationHandle']
        print(f"✅ SQL Submitted. Handle: {op_handle}")
    else:
        raise Exception(f"SQL Submit Failed: {resp.text}")

with DAG(
    'flink_schedule_final_test',  # DAG ID
    start_date=datetime(2023, 1, 1), # 과거 날짜 (필수)
    schedule="*/5 * * * *",          # 5분마다 실행
    catchup=False,                   # 밀린 작업 실행 안 함
    tags=['test', 'flink', 'v3'],
) as dag:

    run_task = PythonOperator(
        task_id='run_every_5_min',
        python_callable=submit_flink_sql
        # provide_context 삭제됨
    )