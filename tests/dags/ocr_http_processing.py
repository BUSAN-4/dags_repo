"""
OCR HTTP API 처리 DAG
- RDS에서 처리 안 된 vehicle_exterior_image 읽기
- Ngrok으로 노출된 OCR API 호출
- 결과를 RDS에 다시 저장
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pymysql
import requests
import logging

# 로거 설정
logger = logging.getLogger(__name__)

# 한국 시간대
KST = ZoneInfo("Asia/Seoul")

# Ngrok URL (팀원이 제공하는 주소로 업데이트 필요)
NGROK_OCR_URL = "https://sherilyn-acerb-wantonly.ngrok-free.dev/ocr/batch"  

# RDS 연결 정보
# DB_CONFIG = {
#     'host': 'busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com',
#     'port': 23306,
#     'user': 'root',
#     'password': 'busan!234pw',
#     'database': 'car_db',
#     'charset': 'utf8mb4'
# }
DB_CONFIG = {
    'host': '172.16.11.114',
    'port': 3307,
    'user': 'root',
    'password': '0000',
    'database': 'busan_car',
    'charset': 'utf8mb4'
}

@dag(
    dag_id='ocr_http_processing',
    description='OCR 이미지 처리 via Ngrok HTTP API',
    schedule='*/5 * * * *',  # 5분마다 실행
    start_date=datetime(2025, 1, 1, tzinfo=KST),
    catchup=False,
    tags=['ocr', 'ngrok', 'image-processing'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
    }
)
def ocr_http_processing_dag():
    """
    OCR HTTP API 처리 DAG
    """
    
    @task
    def process_ocr_images(**context):
        """
        RDS에서 미처리 이미지를 읽어 OCR API 호출 후 결과 저장
        """
        conn = None
        cursor = None
        
        try:
            # RDS 연결
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            # 처리 안 된 이미지 가져오기 (한 번에 최대 50개)
            query = """
                SELECT 
                    image_id,
                    session_id,
                    captured_lat,
                    captured_lon,
                    captured_at,
                    image_base64
                FROM vehicle_exterior_image
                WHERE image_base64 IS NOT NULL
                AND LENGTH(image_base64) > 0
                AND processed = 0
                ORDER BY captured_at DESC
                LIMIT 10
            """
            
            cursor.execute(query)
            images = cursor.fetchall()
            
            if not images:
                logger.info("⏭처리할 이미지가 없습니다.")
                return
            
            logger.info(f"총 {len(images)}개 이미지를 처리합니다.")
            
            # 배열 형태로 payload 준비
            payload_list = []
            for img in images:
                captured_at = img.get('captured_at')
                
                # captured_at을 문자열로 변환 (datetime 객체인 경우)
                if captured_at and hasattr(captured_at, 'strftime'):
                    captured_at = captured_at.strftime('%Y-%m-%d %H:%M:%S')
                
                payload_list.append({
                    "image_id": img['image_id'],
                    "session_id": img['session_id'],
                    "captured_lat": img.get('captured_lat'),
                    "captured_lon": img.get('captured_lon'),
                    "captured_at": captured_at,
                    "image_base64": img['image_base64']
                })
            
            success_count = 0
            fail_count = 0
            
            try:
                # OCR API 호출 (배열 형태로 한 번에 전송)
                logger.info(f"🚀 OCR API 호출 중... ({len(payload_list)}개 이미지)")
                response = requests.post(
                    NGROK_OCR_URL,
                    json=payload_list,  # 배열로 전송
                    timeout=600  # 여러 이미지 처리 시간 고려
                )
                
                if response.status_code == 200:
                    results = response.json()  # 배열 응답 예상
                    
                    # 결과가 배열인지 확인
                    if isinstance(results, list):
                        for result in results:
                            image_id = result.get('image_id', 'unknown')
                            
                            if result.get('status') == 'success':
                                plate_number = result.get('plate_number', '')
                                confidence = result.get('confidence', 0.0)
                                logger.info(f"{image_id}: {plate_number} (신뢰도: {confidence:.2f})")
                                
                                # DB에 처리 완료 표시
                                try:
                                    update_query = """
                                        UPDATE vehicle_exterior_image
                                        SET processed = 1,
                                            updated_at = NOW()
                                        WHERE image_id = %s
                                    """
                                    cursor.execute(update_query, (image_id,))
                                    logger.info(f"처리 완료 표시: {image_id}")
                                    # OCR 결과는 arrears_detection 테이블에 별도 저장 (추후 구현)
                                except Exception as db_error:
                                    logger.error(f"DB 업데이트 실패 ({image_id}): {str(db_error)}")
                                
                                success_count += 1
                            else:
                                logger.warning(f"⚠️ {image_id}: {result.get('message', 'OCR 실패')}")
                                fail_count += 1
                    else:
                        logger.error(f"예상치 못한 응답 형식: {type(results)}")
                        fail_count = len(payload_list)
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    fail_count = len(payload_list)
                    
            except requests.exceptions.Timeout:
                logger.error(f"타임아웃: {len(payload_list)}개 이미지 처리 실패")
                fail_count = len(payload_list)
            except Exception as e:
                logger.error(f"OCR API 호출 실패: {str(e)}")
                fail_count = len(payload_list)
            
            # 결과 요약
            logger.info(f"처리 완료: 성공 {success_count}, 실패 {fail_count}")
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"OCR 처리 중 오류: {str(e)}")
            if conn:
                conn.rollback()
            raise
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    
    # Task 실행
    process_ocr_images()


# DAG 인스턴스 생성
dag_instance = ocr_http_processing_dag()
