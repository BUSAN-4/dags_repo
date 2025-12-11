from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import logging
import pytz

KST = pytz.timezone('Asia/Seoul')
logger = logging.getLogger(__name__)

FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

@dag(
    dag_id='ingest_raw_data',
    description='RDS에서 Kafka로 실시간 데이터 적재 (1분마다)',
    schedule='*/1 * * * *',
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    tags=['flink', 'batch', 'rds-to-kafka'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }
)
def ingest_raw_data():
    
    @task
    def calculate_time_range(**context):
        """현재 실행 시간 기준으로 start_time, end_time 계산 (KST 기준)"""
        execution_date = context.get('logical_date')
        
        # Airflow logical_date(UTC)를 KST로 변환
        execution_date_kst = execution_date.astimezone(KST)
        
        # KST 기준으로 1분 범위 계산
        start_time = (execution_date_kst - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        end_time = execution_date_kst.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"시간 범위 설정 (KST): {start_time} ~ {end_time}")
        logger.info(f"참고 (UTC): {(execution_date - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')} ~ {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
        return {'start_time': start_time, 'end_time': end_time}
    
    @task
    def read_sql_file(time_range: dict):
        """Flink SQL 파일 읽기 및 파라미터 주입"""
        sql_file_path = "/opt/airflow/dags/repo/flink_sql/01_ingest_raw_data.sql"
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 파라미터 주입
            sql_content = sql_content.replace(':start_time', time_range['start_time'])
            sql_content = sql_content.replace(':end_time', time_range['end_time'])
            
            logger.info(f"SQL 파일 읽기 성공: {sql_file_path}")
            logger.info(f"처리 시간: {time_range['start_time']} ~ {time_range['end_time']}")
            return sql_content
            
        except Exception as e:
            logger.error(f"SQL 파일 읽기 실패: {str(e)}")
            raise
    
    @task
    def submit_batch_job(sql_content: str):
        """Flink Batch Job 제출 (세션 생성부터 종료까지)"""
        # 1. 세션 생성
        session_url = f"{FLINK_GATEWAY_URL}/v1/sessions"
        
        try:
            session_response = requests.post(session_url, json={}, timeout=10)
            session_response.raise_for_status()
            session_handle = session_response.json()['sessionHandle']
            logger.info(f"세션 생성 성공: {session_handle}")
        except Exception as e:
            logger.error(f"세션 생성 실패: {str(e)}")
            raise
        
        # 2. SQL 실행
        url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
        
        # SQL 구문 분리
        statements = []
        current_statement = ""
        in_statement_set = False
        
        for line in sql_content.split('\n'):
            line = line.strip()
            
            # 주석 무시
            if line.startswith('--') or not line:
                continue
            
            current_statement += line + " "
            
            # BEGIN STATEMENT SET 감지
            if 'BEGIN STATEMENT SET' in line.upper():
                in_statement_set = True
            
            # END 감지
            if line.upper() == 'END;':
                statements.append(current_statement.strip())
                current_statement = ""
                in_statement_set = False
            # 일반 구문 종료
            elif line.endswith(';') and not in_statement_set:
                statements.append(current_statement.strip())
                current_statement = ""
        
        logger.info(f"총 {len(statements)}개 SQL 구문 실행 예정")
        
        # 각 구문 실행
        for idx, statement in enumerate(statements):
            if not statement:
                continue
            
            try:
                logger.info(f"[{idx+1}/{len(statements)}] SQL 실행 중...")
                
                response = requests.post(
                    url, 
                    json={"statement": statement},
                    timeout=120
                )
                response.raise_for_status()
                result = response.json()
                
                operation_handle = result.get('operationHandle')
                logger.info(f"[{idx+1}/{len(statements)}] 실행 성공: {operation_handle}")
                
            except Exception as e:
                logger.error(f"[{idx+1}/{len(statements)}] 실행 실패: {str(e)}")
                if idx < len(statements) - 1:
                    logger.warning("계속 진행...")
                    continue
                else:
                    raise
        
        # 3. 세션 종료
        close_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}"
        try:
            requests.delete(close_url, timeout=10)
            logger.info(f"세션 종료 성공: {session_handle}")
        except Exception as e:
            logger.warning(f"세션 종료 실패 (무시): {str(e)}")
        
        return {'status': 'completed', 'statements_executed': len(statements), 'session': session_handle}
    
    # Task 흐름
    time_range = calculate_time_range()
    sql_content = read_sql_file(time_range)
    result = submit_batch_job(sql_content)

    return result

# DAG 인스턴스 생성
dag_instance = ingest_raw_data()
