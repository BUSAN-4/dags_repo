"""
Airflow DAG: 기존 데이터 재동기화 (1분 단위 + 5개씩)
- 각 1분 시간 범위에 대해 5개씩 배치로 RDS -> Kafka 전송
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import requests
import time
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Flink SQL Gateway 설정
FLINK_SQL_GATEWAY_URL = "http://flink-sql-gateway-20.flink.svc.cluster.local:8083"
SQL_FILE_PATH = "/opt/airflow/dags/flink_sql/04_resync_batch_limited.sql"

with DAG(
    'resync_batch_limited',
    default_args=default_args,
    description='기존 데이터 재동기화 (1분 단위 + 5개씩)',
    schedule=None,  # 수동 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flink', 'batch', 'resync', 'limited'],
) as dag:

    @task
    def read_sql_file():
        """SQL 파일 읽기"""
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        logging.info(f"✅ SQL 파일 읽기 완료: {SQL_FILE_PATH}")
        return sql_content

    @task
    def calculate_batches(**context):
        """
        처리할 시간 범위 및 배치 개수 계산
        - 1분 단위 시간 범위
        - 각 시간 범위마다 offset을 5씩 증가시키며 처리
        """
        # DAG Run Conf에서 파라미터 가져오기
        conf = context['dag_run'].conf or {}
        start_time_str = conf.get('start_time', '2024-12-01 00:00:00')
        end_time_str = conf.get('end_time', '2024-12-01 00:01:00')
        max_batches_per_minute = conf.get('max_batches_per_minute', 20)  # 최대 100개(5*20)
        
        logging.info(f"📅 시간 범위: {start_time_str} ~ {end_time_str}")
        logging.info(f"📦 최대 배치 수 (1분당): {max_batches_per_minute}")
        
        # 1분 단위로 나눔
        start_dt = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
        
        time_ranges = []
        current_time = start_dt
        
        while current_time < end_dt:
            next_time = current_time + timedelta(minutes=1)
            if next_time > end_dt:
                next_time = end_dt
            
            # 각 1분 범위마다 offset을 0, 5, 10, ..., (max_batches_per_minute-1)*5 까지
            for batch_idx in range(max_batches_per_minute):
                offset = batch_idx * 5
                time_ranges.append({
                    'start_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': next_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'offset': offset,
                    'batch_name': f"{current_time.strftime('%Y%m%d_%H%M')}_offset_{offset}"
                })
            
            current_time = next_time
        
        logging.info(f"✅ 총 {len(time_ranges)}개 배치 생성")
        return time_ranges

    @task
    def submit_batch_job(sql_content: str, batch_info: dict):
        """
        Flink SQL Gateway로 배치 작업 제출
        """
        start_time = batch_info['start_time']
        end_time = batch_info['end_time']
        offset = batch_info['offset']
        batch_name = batch_info['batch_name']
        
        logging.info(f"🚀 배치 시작: {batch_name}")
        logging.info(f"   시간: {start_time} ~ {end_time}")
        logging.info(f"   Offset: {offset} (행 {offset+1}~{offset+5})")
        
        # 세션 생성
        session_resp = requests.post(
            f"{FLINK_SQL_GATEWAY_URL}/v1/sessions",
            json={"properties": {"execution.runtime-mode": "batch"}}
        )
        session_resp.raise_for_status()
        session_handle = session_resp.json()['sessionHandle']
        logging.info(f"🔑 세션 생성: {session_handle}")
        
        try:
            # SQL 파싱 (주석 제거 및 세미콜론으로 분리)
            statements = []
            current_statement = ""
            
            for line in sql_content.split('\n'):
                line = line.strip()
                if line.startswith('--') or not line:
                    continue
                current_statement += line + " "
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
            
            # 각 SQL문 실행
            for idx, stmt in enumerate(statements, 1):
                # 파라미터 치환
                stmt = stmt.replace(':start_time', f"'{start_time}'")
                stmt = stmt.replace(':end_time', f"'{end_time}'")
                stmt = stmt.replace(':offset', str(offset))
                
                logging.info(f"[{idx}/{len(statements)}] SQL 실행 중...")
                
                # SQL 실행
                exec_resp = requests.post(
                    f"{FLINK_SQL_GATEWAY_URL}/v1/sessions/{session_handle}/statements",
                    json={"statement": stmt}
                )
                exec_resp.raise_for_status()
                operation_handle = exec_resp.json()['operationHandle']
                
                # 완료 대기 (INSERT는 FINISHED 상태까지 기다림)
                if stmt.strip().upper().startswith('INSERT'):
                    max_wait = 300  # 최대 5분
                    waited = 0
                    while waited < max_wait:
                        status_resp = requests.get(
                            f"{FLINK_SQL_GATEWAY_URL}/v1/sessions/{session_handle}/operations/{operation_handle}/status"
                        )
                        status = status_resp.json().get('status')
                        
                        if status == 'FINISHED':
                            logging.info(f"✅ [{idx}/{len(statements)}] 완료!")
                            break
                        elif status == 'ERROR':
                            error_msg = status_resp.json().get('error', 'Unknown error')
                            raise Exception(f"SQL 실행 실패: {error_msg}")
                        
                        time.sleep(2)
                        waited += 2
                    
                    if waited >= max_wait:
                        logging.warning(f"⚠️ [{idx}/{len(statements)}] 타임아웃 (5분 초과)")
                else:
                    # CREATE TABLE 등은 즉시 완료로 간주
                    time.sleep(0.5)
                    logging.info(f"✅ [{idx}/{len(statements)}] 완료!")
            
            logging.info(f"✅ 배치 완료: {batch_name}")
            
        finally:
            # 세션 종료
            try:
                requests.delete(f"{FLINK_SQL_GATEWAY_URL}/v1/sessions/{session_handle}")
                logging.info(f"🔒 세션 종료: {session_handle}")
            except Exception as e:
                logging.warning(f"세션 종료 실패: {e}")

    # Task 실행 순서
    sql_content = read_sql_file()
    batches = calculate_batches()
    
    # 각 배치를 순차 실행 (동적 태스크 매핑)
    submit_batch_job.expand(
        sql_content=[sql_content] * 1,  # 모든 배치에 동일한 SQL 전달
        batch_info=batches
    )

