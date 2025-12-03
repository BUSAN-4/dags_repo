from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

# ✅ 방금 확인한 정확한 주소로 설정했습니다.
FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

def submit_flink_sql():
    print(f"Connecting to Flink Gateway at: {FLINK_GATEWAY_URL}")
    
    # 1. 세션 생성
    session_url = f"{FLINK_GATEWAY_URL}/v1/sessions"
    headers = {"Content-Type": "application/json"}
    # 세션 이름은 임의로 설정
    resp = requests.post(session_url, json={"sessionName": "airflow_test_session"}, headers=headers)
    
    if resp.status_code != 200:
        raise Exception(f"Failed to open session. Status: {resp.status_code}, Body: {resp.text}")
    
    session_handle = resp.json()['sessionHandle']
    print(f"✅ Session Created: {session_handle}")

    # 2. SQL 실행 (테스트용 SELECT 1)
    sql = "SELECT 1"
    statement_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
    payload = {"statement": sql}
    
    resp = requests.post(statement_url, json=payload, headers=headers)
    
    if resp.status_code != 200:
        raise Exception(f"Failed to submit SQL. Status: {resp.status_code}, Body: {resp.text}")
    
    operation_handle = resp.json()['operationHandle']
    print(f"✅ SQL Submitted. Operation Handle: {operation_handle}")
    
    # (참고) 결과까지 확인하려면 추가 API 호출이 필요하지만, 
    # 일단 여기까지 성공하면 연결은 완벽한 것입니다.

with DAG(
    'flink_sql_gateway_test',
    schedule=None,  # 수동 실행을 위해 None으로 설정 (또는 timedelta 등 사용 가능)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['flink', 'test'],
) as dag:

    run_sql_task = PythonOperator(
        task_id='run_flink_sql_task',
        python_callable=submit_flink_sql
    )