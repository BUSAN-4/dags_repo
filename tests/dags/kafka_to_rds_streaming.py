from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import logging
import pytz

KST = pytz.timezone('Asia/Seoul')
logger = logging.getLogger(__name__)

FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

@dag(
    dag_id='kafka_to_rds_streaming',
    description='Kafka에서 RDS로 실시간 스트리밍 (24/7 실행)',
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    tags=['flink', 'streaming', 'kafka', 'rds'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
)
def kafka_to_rds_streaming():
    
    @task
    def read_sql_file():
        """Flink SQL 파일 읽기"""
        sql_file_path = "/opt/airflow/dags/repo/flink_sql/03_kafka_to_rds_streaming.sql"
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            logger.info(f"SQL 파일 읽기 성공: {sql_file_path}")
            logger.info(f"SQL 길이: {len(sql_content)} bytes")
            return sql_content
            
        except Exception as e:
            logger.error(f"SQL 파일 읽기 실패: {str(e)}")
            raise
    
    @task
    def create_session():
        """Flink SQL Gateway 세션 생성"""
        url = f"{FLINK_GATEWAY_URL}/v1/sessions"
        
        try:
            response = requests.post(url, json={}, timeout=10)
            response.raise_for_status()
            session_handle = response.json()['sessionHandle']
            
            logger.info(f"세션 생성 성공: {session_handle}")
            return session_handle
            
        except Exception as e:
            logger.error(f"세션 생성 실패: {str(e)}")
            raise
    
    @task
    def submit_streaming_job(session_handle: str, sql_content: str):
        """Flink Streaming Job 제출 (24/7 실행)"""
        url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
        
        # SQL 구문 분리 (SET, CREATE, BEGIN...END 분리)
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
                logger.info(f"SQL: {statement[:100]}...")
                
                response = requests.post(
                    url, 
                    json={"statement": statement},
                    timeout=300
                )
                response.raise_for_status()
                result = response.json()
                
                operation_handle = result.get('operationHandle')
                logger.info(f"[{idx+1}/{len(statements)}] 실행 성공: {operation_handle}")
                
                # STATEMENT SET (스트리밍 잡) 실행 시
                if 'BEGIN STATEMENT SET' in statement.upper():
                    logger.info("실시간 스트리밍 Job 시작됨!")
                    logger.info("Kafka -> RDS 실시간 전송 활성화")
                    logger.info("이 Job은 수동으로 중지할 때까지 계속 실행됩니다")
                    return {
                        'status': 'streaming_started',
                        'operation_handle': operation_handle,
                        'message': 'Streaming job is running continuously'
                    }
                
            except Exception as e:
                logger.error(f"[{idx+1}/{len(statements)}] 실행 실패: {str(e)}")
                if idx < len(statements) - 1:
                    logger.warning("계속 진행...")
                    continue
                else:
                    raise
        
        return {'status': 'completed', 'statements_executed': len(statements)}
    
    @task
    def close_session(session_handle: str):
        """세션 종료 (스트리밍 Job은 계속 실행됨)"""
        url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}"
        
        try:
            response = requests.delete(url, timeout=10)
            logger.info(f"세션 종료: {session_handle}")
            logger.info("스트리밍 Job은 Flink Cluster에서 계속 실행 중입니다")
            
        except Exception as e:
            logger.warning(f"세션 종료 실패 (무시): {str(e)}")
    
    # Task 흐름
    sql_content = read_sql_file()
    session_handle = create_session()
    result = submit_streaming_job(session_handle, sql_content)
    close_session(session_handle)
    
    return result

# DAG 인스턴스 생성
dag_instance = kafka_to_rds_streaming()

