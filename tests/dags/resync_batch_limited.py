from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import logging
import pytz

KST = pytz.timezone('Asia/Seoul')
logger = logging.getLogger(__name__)

FLINK_GATEWAY_URL = "http://sql-gateway-service-20.flink.svc.cluster.local:8083"

@dag(
    dag_id='resync_batch_limited',
    description='RDS 기존 데이터 5개씩 순차 전송 (1분마다)',
    schedule='*/1 * * * *',
    start_date=datetime(2025, 12, 12, tzinfo=KST),
    catchup=False,
    tags=['flink', 'batch', 'resync', 'limited'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }
)
def resync_batch_limited():
    
    @task
    def calculate_offset(**context):
        """
        DAG Run 시간 기반으로 offset 계산
        매분 실행되므로 시간을 기준으로 offset 계산
        """
        execution_date = context.get('logical_date')
        
        # 시작 시간부터 현재까지 몇 분 경과했는지 계산
        start_date = datetime(2025, 12, 12, tzinfo=KST)
        minutes_elapsed = int((execution_date - start_date).total_seconds() / 60)
        
        # offset = 분 경과 * 5
        current_offset = minutes_elapsed * 5
        
        logger.info(f"📊 실행 시간: {execution_date}")
        logger.info(f"📊 경과 시간: {minutes_elapsed}분")
        logger.info(f"📊 현재 offset: {current_offset} (행 {current_offset}~{current_offset+4})")
        
        return current_offset
    
    @task
    def read_sql_file(offset: int):
        """Flink SQL 파일 읽기 및 offset 파라미터 주입"""
        sql_file_path = "/opt/airflow/dags/repo/flink_sql/04_resync_batch_limited.sql"
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8-sig') as f:
                sql_content = f.read()
            
            # offset 파라미터 주입 (순서 중요: offset_end 먼저!)
            offset_start = offset
            offset_end = offset + 5
            
            sql_content = sql_content.replace(':offset_end', str(offset_end))
            sql_content = sql_content.replace(':offset', str(offset_start))
            
            logger.info(f"SQL 파일 읽기 성공: {sql_file_path}")
            logger.info(f"처리 범위: rn > {offset_start} AND rn <= {offset_end}")
            return sql_content
            
        except Exception as e:
            logger.error(f"SQL 파일 읽기 실패: {str(e)}")
            raise
    
    @task
    def submit_batch_job(sql_content: str):
        """Flink Batch Job 제출"""
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
        
        # 2. SQL 구문 분리
        url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/statements"
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
        
        if current_statement.strip():
            statements.append(current_statement.strip())
        
        logger.info(f"총 {len(statements)}개의 SQL 구문 감지")
        
        # 3. SQL 실행
        try:
            for idx, stmt in enumerate(statements, 1):
                logger.info(f"[{idx}/{len(statements)}] SQL 실행 중...")
                
                # INSERT 구문은 전체 SQL 출력
                if stmt.strip().upper().startswith('INSERT'):
                    logger.info(f"전체 SQL: {stmt}")
                else:
                    stmt_preview = stmt[:200] if len(stmt) > 200 else stmt
                    logger.info(f"SQL Preview: {stmt_preview}...")
                
                response = requests.post(url, json={"statement": stmt}, timeout=30)
                response.raise_for_status()
                
                operation_handle = response.json()['operationHandle']
                
                # INSERT 구문 완료 대기
                if stmt.strip().upper().startswith('INSERT'):
                    import time
                    max_wait = 120
                    waited = 0
                    
                    while waited < max_wait:
                        status_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}/operations/{operation_handle}/status"
                        status_response = requests.get(status_url, timeout=10)
                        status = status_response.json().get('status')
                        
                        if status == 'FINISHED':
                            logger.info(f"✅ [{idx}/{len(statements)}] 완료!")
                            break
                        elif status == 'ERROR':
                            # 상세 에러 정보 로그
                            error_response = status_response.json()
                            logger.error(f"❌ Flink SQL 에러 발생:")
                            logger.error(f"Status Response: {error_response}")
                            error_msg = error_response.get('error', {})
                            if isinstance(error_msg, dict):
                                logger.error(f"Error Message: {error_msg.get('message', 'No message')}")
                                logger.error(f"Error Type: {error_msg.get('type', 'No type')}")
                                logger.error(f"Stack Trace: {error_msg.get('stack', 'No stack')}")
                            else:
                                logger.error(f"Error: {error_msg}")
                            raise Exception(f"SQL 실행 실패: {error_msg}")
                        
                        time.sleep(2)
                        waited += 2
                    
                    if waited >= max_wait:
                        logger.warning(f"⚠️ [{idx}/{len(statements)}] 타임아웃")
                else:
                    logger.info(f"✅ [{idx}/{len(statements)}] 완료!")
            
            logger.info("✅ 모든 SQL 실행 완료 (각 테이블당 5개씩)")
            
        except Exception as e:
            logger.error(f"SQL 실행 중 오류: {str(e)}")
            raise
        
        finally:
            # 4. 세션 종료
            try:
                delete_url = f"{FLINK_GATEWAY_URL}/v1/sessions/{session_handle}"
                requests.delete(delete_url, timeout=10)
                logger.info(f"세션 종료: {session_handle}")
            except Exception as e:
                logger.warning(f"세션 종료 실패: {str(e)}")
    
    # DAG 실행 순서
    offset = calculate_offset()
    sql_content = read_sql_file(offset)
    submit_batch_job(sql_content)

# DAG 인스턴스 생성
resync_batch_limited_dag = resync_batch_limited()
