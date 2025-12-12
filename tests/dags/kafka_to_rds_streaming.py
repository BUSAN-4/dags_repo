from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import logging
import pytz
import time

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
    def submit_streaming_job(sql_content: str):
        """Flink Streaming Job 제출 (24/7 실행) - 세션 생성부터 종료까지"""
        # 1. 세션 생성
        session_url = f"{FLINK_GATEWAY_URL}/v1/sessions"
        
        try:
            session_response = requests.post(session_url, json={
                "properties": {
                    "sql-gateway.session.idle-timeout": "365d",
                    "sql-gateway.session.check-interval": "24h"
                }
            }, timeout=10)
            session_response.raise_for_status()
            session_handle = session_response.json()['sessionHandle']
            logger.info(f"세션 생성 성공 (365d timeout): {session_handle}")
        except Exception as e:
            logger.error(f"세션 생성 실패: {str(e)}")
            raise
        
        # 2. SQL 실행
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
            
            # EXECUTE STATEMENT SET 또는 BEGIN STATEMENT SET 감지
            if 'EXECUTE STATEMENT SET' in line.upper() or 'BEGIN STATEMENT SET' in line.upper():
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
                if 'EXECUTE STATEMENT SET' in statement.upper() or 'BEGIN STATEMENT SET' in statement.upper():
                    logger.info("✅ 실시간 스트리밍 Job 시작됨!")
                    logger.info("📊 Kafka -> RDS 실시간 전송 활성화")
                    logger.info(f"🔑 Session: {session_handle}")
                    logger.info(f"⚙️ Operation: {operation_handle}")
                    
                    # 3. Job 상태 모니터링 (1년간 계속 실행)
                    logger.info("⏰ 스트리밍 Job 모니터링 시작 (365일간 실행)")
                    logger.info("🛑 Job을 중지하려면 Flink UI에서 수동으로 취소하세요")
                    
                    # Job이 계속 실행되도록 Task를 살려둠 (365일)
                    sleep_duration = 365 * 24 * 60 * 60  # 1년
                    logger.info(f"💤 {sleep_duration}초 동안 세션 유지...")
                    
                    try:
                        time.sleep(sleep_duration)
                    except Exception as e:
                        logger.warning(f"⚠️ Sleep 중단됨: {str(e)}")
                    
                    return {
                        'status': 'streaming_started',
                        'operation_handle': operation_handle,
                        'session': session_handle,
                        'message': 'Streaming job ran for 1 year'
                    }
                
            except Exception as e:
                logger.error(f"[{idx+1}/{len(statements)}] 실행 실패: {str(e)}")
                if idx < len(statements) - 1:
                    logger.warning("계속 진행...")
                    continue
                else:
                    raise
        
        # 4. 일반 종료 (스트리밍이 아닌 경우)
        close_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}"
        try:
            requests.delete(close_url, timeout=10)
            logger.info(f"세션 종료 성공: {session_handle}")
        except Exception as e:
            logger.warning(f"세션 종료 실패 (무시): {str(e)}")
        
        return {'status': 'completed', 'statements_executed': len(statements), 'session': session_handle}
    
    # Task 흐름
    sql_content = read_sql_file()
    result = submit_streaming_job(sql_content)
    
    return result

# DAG 인스턴스 생성
dag_instance = kafka_to_rds_streaming()




