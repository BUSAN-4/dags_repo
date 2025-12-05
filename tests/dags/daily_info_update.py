"""
일일 정보 갱신 DAG (arrears_info, missing_person_info)
- 매일 00:00에 실행
- Flink SQL로 RDS → Kafka 전송
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 한국 시간대
KST = ZoneInfo("Asia/Seoul")


@dag(
    dag_id='daily_info_update',
    description='체납/실종자 정보 일일 갱신',
    schedule='0 0 * * *',  # 매일 00:00
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    tags=['flink', 'batch', 'daily'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def daily_info_update_dag():
    """
    체납/실종자 정보 일일 갱신 DAG
    """
    
    @task
    def run_daily_info_update():
        """
        Flink SQL 실행
        """
        import subprocess
        
        print("🔄 일일 정보 갱신 시작...")
        
        # Flink SQL 실행 명령어
        cmd = """
        kubectl exec -i -n flink flink-sql-client-<POD_NAME> -- \
        /opt/flink/bin/sql-client.sh embedded \
        -f /opt/airflow/dags/flink_sql/02_daily_info_update.sql
        """
        
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ 일일 정보 갱신 완료")
            print(result.stdout)
        else:
            print("❌ 일일 정보 갱신 실패")
            print(result.stderr)
            raise Exception(f"Flink 작업 실패: {result.stderr}")
        
        return result.returncode
    
    
    # Task 실행
    run_daily_info_update()


# DAG 인스턴스 생성
dag_instance = daily_info_update_dag()
