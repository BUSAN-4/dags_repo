"""
실시간 데이터 적재 DAG (RDS → Kafka)
- 1분마다 실행
- Flink SQL로 시간 범위별 데이터 전송
- Airflow 파라미터: start_time, end_time
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 한국 시간대 설정
KST = ZoneInfo("Asia/Seoul")

# 한국 시간대 설정
KST = ZoneInfo("Asia/Seoul")


@dag(
    dag_id='ingest_raw_data',
    description='RDS → Kafka 실시간 데이터 적재',
    schedule='*/1 * * * *',  # 1분마다 실행
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    tags=['flink', 'streaming', 'realtime'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }
)
def ingest_raw_data_dag():
    """
    RDS → Kafka 실시간 데이터 적재 DAG
    """
    
    @task
    def calculate_time_range(**context):
        """
        현재 실행 시간 기준으로 start_time, end_time 계산
        예: 14:05:00 실행 → start: 14:04:00, end: 14:05:00
        """
        from airflow.models import Variable
        
        execution_date = context['logical_date']  # Airflow 3.0: execution_date → logical_date
        
        # 1분 전부터 현재까지
        start_time = (execution_date - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        end_time = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"✅ 시간 범위 설정: {start_time} ~ {end_time}")
        return {'start_time': start_time, 'end_time': end_time}
    
    
    @task
    def run_flink_ingest(time_range: dict):
        """
        Flink SQL 실행
        """
        import subprocess
        
        start_time = time_range['start_time']
        end_time = time_range['end_time']
        
        print(f"📅 처리 시간 범위: {start_time} ~ {end_time}")
        
        # Flink SQL 실행 명령어
        cmd = f"""
        cat /opt/airflow/dags/flink_sql/01_ingest_raw_data.sql | \
        sed "s/:start_time/{start_time}/g" | \
        sed "s/:end_time/{end_time}/g" | \
        kubectl exec -i -n flink flink-sql-client-<POD_NAME> -- \
        /opt/flink/bin/sql-client.sh embedded
        """
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Flink 작업 완료")
            print(result.stdout)
        else:
            print("❌ Flink 작업 실패")
            print(result.stderr)
            raise Exception(f"Flink 작업 실패: {result.stderr}")
        
        return result.returncode
    
    
    # Task 의존성
    time_range = calculate_time_range()
    run_flink_ingest(time_range)


# DAG 인스턴스 생성
dag_instance = ingest_raw_data_dag()

