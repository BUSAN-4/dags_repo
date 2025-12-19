"""
Paddle OCR FastAPI 서비스
Kubernetes 환경에서 실행되는 차량 번호판 인식 API
"""

import os
import time
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import cv2
import numpy as np
from ultralytics import YOLO
from paddleocr import PaddleOCR
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge

# 로깅 설정 (먼저 정의!)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka 관련 import (선택적) - HTTP 모드 사용 시 불필요
try:
    from kafka import KafkaConsumer, KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    # HTTP 모드만 사용하므로 경고 생략

# 데이터베이스 관련 import (선택적) - HTTP 모드에서도 체납자 조회용으로 사용 가능
try:
    from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Float, text
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    # DB 연결 선택적 - 없어도 기본 OCR 기능 작동

# 메트릭 설정 (중복 등록 방지)
def get_or_create_metric(metric_class, name, description, labelnames=None, registry=None):
    """메트릭 생성 또는 기존 것 반환 (중복 방지)"""
    from prometheus_client import REGISTRY as DEFAULT_REGISTRY
    reg = registry or DEFAULT_REGISTRY
    
    try:
        # 이미 등록된 메트릭이 있는지 확인
        for collector in list(reg._collector_to_names.keys()):
            if hasattr(collector, '_name') and collector._name == name:
                return collector
    except:
        pass
    
    # 새로운 메트릭 생성
    try:
        if labelnames:
            return metric_class(name, description, labelnames)
        else:
            return metric_class(name, description)
    except ValueError:
        # 그래도 중복이면 더미 메트릭 반환
        class DummyMetric:
            def inc(self, *args, **kwargs): pass
            def observe(self, *args, **kwargs): pass
            def set(self, *args, **kwargs): pass
            def labels(self, *args, **kwargs): return self
        return DummyMetric()

REQUEST_COUNT = get_or_create_metric(Counter, 'paddle_ocr_requests_total', 'Total requests', ['method', 'endpoint'])
REQUEST_LATENCY = get_or_create_metric(Histogram, 'paddle_ocr_request_duration_seconds', 'Request duration', ['method', 'endpoint'])
ACTIVE_REQUESTS = get_or_create_metric(Gauge, 'paddle_ocr_active_requests', 'Active requests')

# 데이터베이스 모델 (선택적)
if DATABASE_AVAILABLE:
    Base = declarative_base()

class ArrearsInfo(Base):
    """체납자 정보 테이블 모델"""
    __tablename__ = "arrears_info"

    car_plate_number = Column(String(20), primary_key=True, comment='차량번호(PK)')
    arrears_user_id = Column(String(50), comment='체납자 ID')
    total_arrears_amount = Column(Integer, comment='총 체납액')
    arrears_period = Column(String(100), comment='체납 기간')
    notice_sent = Column(Boolean, default=False, comment='고지서 발송 여부')
    updated_at = Column(DateTime, comment='변경 시각')

class ArrearsDetection(Base):
    """체납 차량 AI 탐지 결과 테이블 모델"""
    __tablename__ = "arrears_detection"

    detection_id = Column(String(64), primary_key=True, comment='탐지 ID(PK)')
    image_id = Column(String(64), comment='이미지 ID (FK)')
    car_plate_number = Column(String(20), comment='차량번호 (FK)')
    detection_success = Column(Boolean, default=True, comment='탐지 성공 여부')
    detected_lat = Column(Float, comment='탐지된 위도')
    detected_lon = Column(Float, comment='탐지된 경도')
    detected_time = Column(DateTime, comment='탐지 시간')

# 모델 전역 변수
vehicle_detector = None
plate_detector = None
ocr_model = None

# 데이터베이스 전역 변수
db_engine = None
db_session_maker = None

class OCRResult(BaseModel):
    """OCR 결과 모델"""
    vehicle_id: int
    plate_id: int
    license_plate: str
    confidence: float
    yolo_confidence: float
    detected_at: str
    processing_time: float

class BatchOCRResponse(BaseModel):
    """배치 OCR 응답 모델"""
    results: List[OCRResult]
    total_vehicles: int
    total_plates: int
    total_validated: int
    processing_time: float
    status: str
    arrears_detections: Optional[Dict] = None  # 체납자 감지 결과 (선택적)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 라이프사이클"""
    global vehicle_detector, plate_detector, ocr_model, db_engine, db_session_maker

    # 시작 시 모델 로드
    logger.info("🚀 모델 로딩 시작...")
    try:
        vehicle_detector = YOLO("car_detect_model/yolov8m.pt")
        plate_detector = YOLO("plate_model/best.pt")
        ocr_model = PaddleOCR(
            det=False,
            rec=True,
            rec_model_dir="car_plate_number_model_inference_data",
            use_angle_cls=False,
            lang='korean',
            use_gpu=False
        )
        logger.info("✅ 모든 모델 로딩 완료")
    except Exception as e:
        logger.error(f"❌ 모델 로딩 실패: {e}")
        raise

    # 데이터베이스 연결 초기화 (선택적)
    if DATABASE_AVAILABLE:
        try:
            # 환경 변수에서 데이터베이스 설정 가져오기
            db_host = os.getenv("DB_HOST", "172.16.11.114")
            db_port = os.getenv("DB_PORT", "3307")
            db_user = os.getenv("DB_USER", "root")
            db_password = os.getenv("DB_PASSWORD", "0000")
            db_name = os.getenv("DB_NAME", "busan_db")

            db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            db_engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300)
            db_session_maker = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

            # 연결 테스트
            with db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ 데이터베이스 연결 완료")
        except Exception as e:
            logger.error(f"❌ 데이터베이스 연결 실패: {e}")
            logger.warning("⚠️ 체납자 조회 기능이 비활성화됩니다.")

    yield

    # 종료 시 정리
    if db_session_maker:
        db_session_maker.close_all()
    if db_engine:
        db_engine.dispose()
    logger.info("🛑 애플리케이션 종료")

# FastAPI 앱 생성
app = FastAPI(
    title="Paddle OCR Service",
    description="차량 번호판 인식 API 서비스 (HTTP 모드)",
    version="1.0.0",
    lifespan=lifespan
)

@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 시 추가 설정"""
    logger.info("🌐 HTTP API 모드로 실행")
    # Kafka 기능은 사용하지 않음 (HTTP 전용)

# Prometheus 메트릭 엔드포인트 추가
app.add_route("/metrics", prometheus_client.make_asgi_app())

@app.get("/health")
async def health_check():
    """헬스체크 엔드포인트"""
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {"message": "Paddle OCR Service is running", "version": "1.0.0"}

@app.post("/ocr", response_model=BatchOCRResponse)
async def process_image(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    min_confidence: float = 0.5,
    yolo_conf: float = 0.25,
    camera_id: Optional[str] = None,
    location: Optional[str] = None
):
    """단일 이미지 파일로 OCR 처리"""
    return await _process_image_file(file, min_confidence, yolo_conf, camera_id, location)

@app.post("/ocr/batch", response_model=BatchOCRResponse)
async def process_image_batch(
    background_tasks: BackgroundTasks,
    image_data: List[Dict],  # vehicle_exterior_image 테이블 데이터
    min_confidence: float = 0.5,
    yolo_conf: float = 0.25
):
    """배치 이미지 데이터로 OCR 처리 (vehicle_exterior_image 테이블 형식)"""
    return await _process_image_batch(image_data, min_confidence, yolo_conf)

async def _process_image_file(
    file: UploadFile,
    min_confidence: float,
    yolo_conf: float,
    camera_id: Optional[str],
    location: Optional[str]
) -> BatchOCRResponse:
    """단일 이미지 파일 처리"""
    REQUEST_COUNT.labels(method='POST', endpoint='/ocr').inc()
    ACTIVE_REQUESTS.inc()

    start_time = time.time()

    try:
        # 파일 형식 검증
        if not file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
            raise HTTPException(400, "지원하지 않는 파일 형식입니다")

        # 이미지 읽기
        contents = await file.read()
        nparr = np.frombuffer(contents, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            raise HTTPException(400, "이미지를 읽을 수 없습니다")

        # OCR 처리
        results = await process_ocr_pipeline(img, min_confidence, yolo_conf)

        # 체납자 감지
        arrears_detections = []
        for result in results:
            detection_data = {
                'detected_at': result.detected_at,
                'camera_id': camera_id,
                'location': location
            }

            arrears_detection = check_arrears_and_notify(
                result.license_plate,
                detection_data
            )

            if arrears_detection:
                arrears_detections.append(arrears_detection)

        processing_time = time.time() - start_time

        REQUEST_LATENCY.labels(method='POST', endpoint='/ocr').observe(processing_time)

        # 응답에 체납자 정보 포함
        response_data = BatchOCRResponse(
            results=results,
            total_vehicles=len(set(r.vehicle_id for r in results)),
            total_plates=len(results),
            total_validated=len([r for r in results if r.confidence >= min_confidence]),
            processing_time=processing_time,
            status="success"
        )

        response_data.arrears_detections = {
            'detections': arrears_detections,
            'total_arrears_found': len(arrears_detections),
            'notifications_required': len([d for d in arrears_detections if d.get('notification_required', False)])
        }

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OCR 처리 중 오류: {e}")
        raise HTTPException(500, f"처리 중 오류 발생: {str(e)}")
    finally:
        ACTIVE_REQUESTS.dec()

async def _process_image_batch(
    image_data: List[Dict],
    min_confidence: float,
    yolo_conf: float
) -> BatchOCRResponse:
    """배치 이미지 데이터 처리 (vehicle_exterior_image 테이블 형식)"""
    REQUEST_COUNT.labels(method='POST', endpoint='/ocr/batch').inc()
    ACTIVE_REQUESTS.inc()

    start_time = time.time()

    try:
        if not image_data:
            raise HTTPException(400, "이미지 데이터가 비어있습니다")

        logger.info(f"📦 배치 처리 시작: {len(image_data)}개 이미지")

        batch_results = []
        total_processing_time = 0

        for image_row in image_data:
            result = await process_vehicle_image_row(image_row, f"batch_{len(batch_results)}")
            if result:
                batch_results.append(result)
                total_processing_time += result['processing_time']

        if not batch_results:
            raise HTTPException(400, "처리 가능한 이미지가 없습니다")

        # 모든 결과 합치기
        all_ocr_results = []
        all_arrears_detections = []

        for batch_result in batch_results:
            all_ocr_results.extend(batch_result['ocr_results'])
            all_arrears_detections.extend(batch_result['arrears_detections'])

        processing_time = time.time() - start_time

        REQUEST_LATENCY.labels(method='POST', endpoint='/ocr/batch').observe(processing_time)

        # OCRResult 객체들 생성
        ocr_results = []
        for result_dict in all_ocr_results:
            ocr_result = OCRResult(
                vehicle_id=result_dict['vehicle_id'],
                plate_id=result_dict['plate_id'],
                license_plate=result_dict['license_plate'],
                confidence=result_dict['confidence'],
                yolo_confidence=result_dict['yolo_confidence'],
                detected_at=result_dict['detected_at'],
                processing_time=result_dict.get('processing_time', 0)
            )
            ocr_results.append(ocr_result)

        # 응답 구성
        response_data = BatchOCRResponse(
            results=ocr_results,
            total_vehicles=len(set(r.vehicle_id for r in ocr_results)),
            total_plates=len(ocr_results),
            total_validated=len([r for r in ocr_results if r.confidence >= min_confidence]),
            processing_time=processing_time,
            status="success"
        )

        response_data.arrears_detections = {
            'detections': all_arrears_detections,
            'total_arrears_found': len(all_arrears_detections),
            'notifications_required': len([d for d in all_arrears_detections if d.get('notification_required', False)]),
            'batch_info': {
                'total_images': len(image_data),
                'processed_images': len(batch_results),
                'session_ids': list(set(r['session_id'] for r in batch_results if r.get('session_id')))
            }
        }

        logger.info(f"✅ 배치 처리 완료: {len(batch_results)}/{len(image_data)}개 이미지 처리됨")
        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"배치 처리 중 오류: {e}")
        raise HTTPException(500, f"처리 중 오류 발생: {str(e)}")
    finally:
        ACTIVE_REQUESTS.dec()
    """이미지에서 번호판 인식"""
    REQUEST_COUNT.labels(method='POST', endpoint='/ocr').inc()
    ACTIVE_REQUESTS.inc()

    start_time = time.time()

    try:
        # 파일 검증
        if not file.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
            raise HTTPException(400, "지원하지 않는 파일 형식입니다")

        # 이미지 읽기
        contents = await file.read()
        nparr = np.frombuffer(contents, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            raise HTTPException(400, "이미지를 읽을 수 없습니다")

        # OCR 처리 (pipeline_test2.py 로직 기반)
        results = await process_ocr_pipeline(img, min_confidence, yolo_conf)

        # 체납자 감지
        arrears_detections = []
        for result in results:
            detection_data = {
                'detected_at': result.detected_at,
                'camera_id': camera_id,
                'location': location
            }

            arrears_detection = check_arrears_and_notify(
                result.license_plate,
                detection_data
            )

            if arrears_detection:
                arrears_detections.append(arrears_detection)

        processing_time = time.time() - start_time

        REQUEST_LATENCY.labels(method='POST', endpoint='/ocr').observe(processing_time)

        # 응답에 체납자 정보 포함
        response_data = BatchOCRResponse(
            results=results,
            total_vehicles=len(set(r.vehicle_id for r in results)),
            total_plates=len(results),
            total_validated=len([r for r in results if r.confidence >= min_confidence]),
            processing_time=processing_time,
            status="success"
        )

        # 체납자 정보를 추가 속성으로 포함
        response_data.arrears_detections = {
            'detections': arrears_detections,
            'total_arrears_found': len(arrears_detections),
            'notifications_required': len([d for d in arrears_detections if d.get('notification_required', False)])
        }

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OCR 처리 중 오류: {e}")
        raise HTTPException(500, f"처리 중 오류 발생: {str(e)}")
    finally:
        ACTIVE_REQUESTS.dec()

async def process_ocr_pipeline(img: np.ndarray, min_confidence: float, yolo_conf: float) -> List[OCRResult]:
    """OCR 파이프라인 처리"""
    global vehicle_detector, plate_detector, ocr_model

    results = []
    vehicle_count = 0

    # 차량 탐지
    vehicle_results = vehicle_detector(img, conf=yolo_conf)

    for vehicle_box in vehicle_results[0].boxes:
        cls = int(vehicle_box.cls[0])
        if cls not in [2, 5, 7]:  # car, bus, truck
            continue

        vehicle_count += 1
        vx1, vy1, vx2, vy2 = map(int, vehicle_box.xyxy[0])
        vehicle_crop = img[vy1:vy2, vx1:vx2]

        # 번호판 탐지
        plate_results = plate_detector(vehicle_crop, conf=0.10)

        if len(plate_results[0].boxes) == 0:
            continue

        # YOLO 신뢰도로 정렬
        plate_boxes = []
        for pbox in plate_results[0].boxes:
            px1, py1, px2, py2 = map(int, pbox.xyxy[0])
            plate_crop = vehicle_crop[py1:py2, px1:px2]
            if plate_crop.shape[0] < 10 or plate_crop.shape[1] < 10:
                continue
            plate_boxes.append((plate_crop, float(pbox.conf[0])))

        if not plate_boxes:
            continue

        # 최고 신뢰도 번호판 선택
        plate_boxes.sort(key=lambda x: x[1], reverse=True)
        best_plate_crop, yolo_confidence = plate_boxes[0]

        # OCR 실행
        try:
            ocr_result = ocr_model.ocr(best_plate_crop, det=False, cls=False)
            if ocr_result and ocr_result[0]:
                text = ocr_result[0][0][0]
                confidence = ocr_result[0][0][1]
            else:
                text = "인식 실패"
                confidence = 0.0
        except Exception as e:
            logger.warning(f"OCR 오류: {e}")
            text = "OCR 오류"
            confidence = 0.0

        # 결과 저장
        result = OCRResult(
            vehicle_id=vehicle_count,
            plate_id=len(results) + 1,
            license_plate=text,
            confidence=float(confidence),
            yolo_confidence=float(yolo_confidence),
            detected_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            processing_time=time.time() - time.time()  # 개별 처리 시간 측정 필요시 수정
        )
        results.append(result)

    return results

# 차량 이미지 처리 함수들
async def process_vehicle_image_row(image_row: dict, request_id: str) -> Optional[dict]:
    """vehicle_exterior_image 테이블의 한 행을 처리"""
    try:
        image_id = image_row.get('image_id')
        session_id = image_row.get('session_id')
        captured_lat = image_row.get('captured_lat')
        captured_lon = image_row.get('captured_lon')
        captured_at = image_row.get('captured_at')
        image_base64 = image_row.get('image_base64')

        if not image_base64:
            logger.warning(f"⚠️ Base64 이미지 데이터 없음: {image_id}")
            return None

        # Base64 디코딩
        import base64
        try:
            img_bytes = base64.b64decode(image_base64)
            nparr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        except Exception as e:
            logger.error(f"❌ 이미지 디코딩 실패: {image_id} - {e}")
            return None

        if img is None:
            logger.error(f"❌ 이미지 디코딩 결과 None: {image_id}")
            return None

        # OCR 처리
        min_confidence = float(os.getenv("MIN_CONFIDENCE", "0.5"))
        yolo_conf = float(os.getenv("YOLO_CONF", "0.25"))

        ocr_start_time = time.time()
        ocr_results = await process_ocr_pipeline(img, min_confidence, yolo_conf)
        ocr_processing_time = time.time() - ocr_start_time

        # 체납자 감지
        arrears_detections = []
        for result in ocr_results:
            detection_data = {
                'detected_at': result.detected_at,
                'session_id': session_id,
                'image_id': image_id,
                'captured_lat': captured_lat,
                'captured_lon': captured_lon,
                'captured_at': captured_at
            }

            arrears_detection = check_arrears_and_notify(
                result.license_plate,
                detection_data
            )

            if arrears_detection:
                arrears_detections.append(arrears_detection)
                # 체납자 감지 결과는 DB에 저장하지 않고 Kafka로만 전송

        # OCR 결과 변환 (Pydantic → dict)
        ocr_results_dict = []
        for r in ocr_results:
            result_dict = r.dict()
            result_dict['session_id'] = session_id
            result_dict['image_id'] = image_id
            result_dict['captured_lat'] = captured_lat
            result_dict['captured_lon'] = captured_lon
            result_dict['captured_at'] = captured_at
            ocr_results_dict.append(result_dict)

        logger.info(f"✅ 이미지 처리 완료: {image_id} ({len(ocr_results)}개 번호판, {len(arrears_detections)}개 체납자)")

        return {
            'image_id': image_id,
            'session_id': session_id,
            'ocr_results': ocr_results_dict,
            'arrears_detections': arrears_detections,
            'processing_time': ocr_processing_time,
            'captured_at': captured_at
        }

    except Exception as e:
        logger.error(f"❌ 이미지 행 처리 실패: {e}")
        return None

async def process_arrears_detection_message(message_data: dict, request_id: str):
    """arrears_detection 토픽의 메시지를 처리하여 데이터베이스에 저장"""
    try:
        detection_id = message_data.get('detection_id')
        image_id = message_data.get('image_id')
        car_plate_number = message_data.get('car_plate_number')
        detection_success = message_data.get('detection_success', True)
        detected_lat = message_data.get('detected_lat')
        detected_lon = message_data.get('detected_lon')
        detected_time = message_data.get('detected_time')

        if not all([detection_id, image_id, car_plate_number]):
            logger.warning(f"⚠️ 필수 필드가 누락됨: detection_id={detection_id}, image_id={image_id}, car_plate_number={car_plate_number}")
            return

        # 데이터베이스에 저장
        if DATABASE_AVAILABLE and db_session_maker:
            try:
                with db_session_maker() as session:
                    # 이미 존재하는지 확인
                    existing = session.query(ArrearsDetection).filter(
                        ArrearsDetection.detection_id == detection_id
                    ).first()

                    if existing:
                        logger.info(f"ℹ️ 이미 존재하는 탐지 결과: {detection_id}")
                        return

                    # 새로운 레코드 생성
                    detection_record = ArrearsDetection(
                        detection_id=detection_id,
                        image_id=image_id,
                        car_plate_number=car_plate_number,
                        detection_success=bool(detection_success),
                        detected_lat=detected_lat,
                        detected_lon=detected_lon,
                        detected_time=detected_time
                    )

                    session.add(detection_record)
                    session.commit()

                    logger.info(f"✅ 체납자 탐지 결과 저장 완료: {detection_id} ({car_plate_number})")

            except Exception as e:
                logger.error(f"❌ 데이터베이스 저장 실패: {e}")
        else:
            logger.warning("⚠️ 데이터베이스가 연결되지 않아 탐지 결과를 저장할 수 없습니다.")

    except Exception as e:
        logger.error(f"❌ 체납자 탐지 메시지 처리 실패: {e}")

# 체납자 조회 관련 함수들
def get_arrears_info(license_plate: str) -> Optional[dict]:
    """번호판으로 체납자 정보 조회"""
    if not DATABASE_AVAILABLE or not db_session_maker:
        return None

    try:
        # 번호판 정규화 (공백, 하이픈 제거)
        normalized_plate = license_plate.replace(' ', '').replace('-', '').upper()

        with db_session_maker() as session:
            arrears = session.query(ArrearsInfo).filter(
                ArrearsInfo.car_plate_number == normalized_plate
            ).first()

            if arrears:
                return {
                    'car_plate_number': arrears.car_plate_number,
                    'arrears_user_id': arrears.arrears_user_id,
                    'total_arrears_amount': arrears.total_arrears_amount,
                    'arrears_period': arrears.arrears_period,
                    'notice_sent': arrears.notice_sent,
                    'updated_at': arrears.updated_at.isoformat() if arrears.updated_at else None
                }
    except Exception as e:
        logger.error(f"체납자 정보 조회 실패 ({license_plate}): {e}")

    return None

def save_arrears_detection(detection_data: dict) -> bool:
    """체납자 감지 결과를 arrears_detection 테이블에 저장"""
    if not DATABASE_AVAILABLE or not db_session_maker:
        logger.warning("⚠️ 데이터베이스가 연결되지 않아 탐지 결과를 저장할 수 없습니다.")
        return False

    try:
        # detection_id 생성 (timestamp + image_id + plate 조합)
        timestamp = int(time.time() * 1000000)  # 마이크로초 단위
        detection_id = f"det_{timestamp}_{detection_data['image_id']}_{detection_data['license_plate'].replace(' ', '_')}"

        with db_session_maker() as session:
            detection_record = ArrearsDetection(
                detection_id=detection_id,
                image_id=detection_data['image_id'],
                car_plate_number=detection_data['license_plate'],
                detection_success=True,  # 항상 True
                detected_lat=detection_data.get('captured_lat'),
                detected_lon=detection_data.get('captured_lon'),
                detected_time=detection_data.get('detected_at')
            )

            session.add(detection_record)
            session.commit()

            logger.info(f"✅ 체납자 탐지 결과 저장: {detection_id} ({detection_data['license_plate']})")
            return True

    except Exception as e:
        logger.error(f"❌ 체납자 탐지 결과 저장 실패: {e}")
        return False

def check_arrears_and_notify(license_plate: str, detection_data: dict) -> Optional[dict]:
    """체납자 확인 및 알림 데이터 생성"""
    arrears_info = get_arrears_info(license_plate)

    if arrears_info:
        return {
            'detected_at': detection_data.get('detected_at'),
            'session_id': detection_data.get('session_id'),
            'image_id': detection_data.get('image_id'),
            'camera_id': detection_data.get('camera_id'),
            'location': detection_data.get('location'),
            'captured_lat': detection_data.get('captured_lat'),
            'captured_lon': detection_data.get('captured_lon'),
            'captured_at': detection_data.get('captured_at'),
            'license_plate': license_plate,
            'arrears_info': arrears_info,
            'notification_required': not arrears_info.get('notice_sent', False),
            'severity': 'high' if arrears_info.get('total_arrears_amount', 0) > 100000 else 'medium'
        }

    return None

# Kafka Consumer 관련 함수들
async def run_kafka_consumer():
    """Kafka Consumer 실행 (비동기)"""
    if not KAFKA_AVAILABLE:
        logger.error("❌ Kafka 패키지가 설치되지 않았습니다.")
        return

    global vehicle_detector, plate_detector, ocr_model

    # Kafka 설정
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "vehicle_images")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "ocr_results")
    arrears_input_topic = os.getenv("KAFKA_ARREARS_INPUT_TOPIC", "arrears_detection")
    group_id = os.getenv("KAFKA_GROUP_ID", "paddle_ocr_consumers")

    logger.info(f"🔄 Kafka Consumer 시작: {kafka_servers} / 토픽: {input_topic}")

    try:
        # Consumer 설정 (두 토픽 구독)
        consumer = KafkaConsumer(
            input_topic,
            arrears_input_topic,
            bootstrap_servers=[kafka_servers],
            group_id=group_id,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000  # 타임아웃 설정
        )

        # Producer 설정
        producer = KafkaProducer(
            bootstrap_servers=[kafka_servers],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            retries=3
        )

        logger.info("✅ Kafka Consumer/Producer 연결 완료")

        while True:
            try:
                # 메시지 폴링 (비동기)
                message_batch = consumer.poll(timeout_ms=1000)

                for topic_partition, messages in message_batch.items():
                    topic_name = topic_partition.topic

                    for message in messages:
                        try:
                            message_data = message.value
                            request_id = message_data.get('request_id', f"msg_{message.offset}")

                            logger.info(f"📥 메시지 수신 [{topic_name}]: {request_id}")

                            # 토픽별 처리 분기
                            if topic_name == arrears_input_topic:
                                # arrears_detection 토픽 처리
                                await process_arrears_detection_message(message_data, request_id)
                                continue

                            # 배치 데이터 처리 (vehicle_exterior_image 테이블 데이터)
                            if isinstance(message_data, list):
                                # 배치 처리
                                logger.info(f"📦 배치 데이터 수신: {len(message_data)}개 이미지")
                                min_confidence = float(os.getenv("MIN_CONFIDENCE", "0.5"))
                                batch_results = []

                                for image_row in message_data:
                                    result = await process_vehicle_image_row(image_row, request_id)
                                    if result:
                                        batch_results.append(result)

                                if not batch_results:
                                    continue

                                # 배치 결과 구성
                                all_ocr_results = []
                                all_arrears_detections = []

                                for batch_result in batch_results:
                                    all_ocr_results.extend(batch_result['ocr_results'])
                                    all_arrears_detections.extend(batch_result['arrears_detections'])

                                output_message = {
                                    'request_id': request_id,
                                    'batch_size': len(message_data),
                                    'processed_count': len(batch_results),
                                    'ocr_result': {
                                        'results': all_ocr_results,
                                        'total_vehicles': len(set(r['vehicle_id'] for r in all_ocr_results)),
                                        'total_plates': len(all_ocr_results),
                                        'total_validated': len([r for r in all_ocr_results if r.get('confidence', 0) >= min_confidence]),
                                        'processing_time': sum(r['processing_time'] for r in batch_results),
                                        'status': 'success'
                                    },
                                    'arrears_detections': {
                                        'detections': all_arrears_detections,
                                        'total_arrears_found': len(all_arrears_detections),
                                        'notifications_required': len([d for d in all_arrears_detections if d.get('notification_required', False)])
                                    },
                                    'processed_at': time.time(),
                                    'batch_info': {
                                        'session_ids': list(set(r['session_id'] for r in batch_results if r.get('session_id'))),
                                        'time_range': {
                                            'start': min((r['captured_at'] for r in batch_results if r.get('captured_at')), default=None),
                                            'end': max((r['captured_at'] for r in batch_results if r.get('captured_at')), default=None)
                                        }
                                    }
                                }

                            else:
                                # 단일 이미지 처리 (기존 방식)
                                result = await process_vehicle_image_row(message_data, request_id)
                                if not result:
                                    continue

                                output_message = {
                                    'request_id': request_id,
                                    'ocr_result': result['ocr_result'],
                                    'arrears_detections': {
                                        'detections': result['arrears_detections'],
                                        'total_arrears_found': len(result['arrears_detections']),
                                        'notifications_required': len([d for d in result['arrears_detections'] if d.get('notification_required', False)])
                                    },
                                    'processed_at': time.time(),
                                    'metadata': message_data.get('metadata', {})
                                }

                            # 체납자 감지 결과 별도 발행
                            arrears_detections = all_arrears_detections if isinstance(message_data, list) else result['arrears_detections']
                            ocr_results_count = len(all_ocr_results) if isinstance(message_data, list) else len(result['ocr_results'])

                            if arrears_detections:
                                arrears_topic = os.getenv("KAFKA_ARREARS_TOPIC", "arrears_detections")
                                arrears_message = {
                                    'request_id': request_id,
                                    'detections': arrears_detections,
                                    'total_arrears': len(arrears_detections),
                                    'camera_id': message_data[0].get('camera_id') if isinstance(message_data, list) else message_data.get('camera_id'),
                                    'location': message_data[0].get('location') if isinstance(message_data, list) else message_data.get('location'),
                                    'detected_at': time.time()
                                }

                                producer.send(arrears_topic, arrears_message)
                                producer.flush()

                                logger.info(f"🚨 체납자 감지 및 알림 발행: {request_id} ({len(arrears_detections)}건)")

                            logger.info(f"✅ OCR 완료 및 결과 발행: {request_id} ({ocr_results_count}개 번호판)")

                        except Exception as e:
                            logger.error(f"❌ 메시지 처리 실패: {e}")
                            # 에러 메시지 발행 (선택적)
                            error_message = {
                                'request_id': message_data.get('request_id', 'unknown') if not isinstance(message_data, list) else f"batch_{len(message_data)}_error",
                                'error': str(e),
                                'error_type': type(e).__name__,
                                'processed_at': time.time()
                            }
                            try:
                                producer.send(f"{output_topic}_errors", error_message)
                            except:
                                pass

                # CPU 사용률 조절을 위한 짧은 대기
                await asyncio.sleep(0.01)

            except Exception as e:
                logger.error(f"❌ Kafka Consumer 오류: {e}")
                await asyncio.sleep(1)  # 재연결 대기

    except KeyboardInterrupt:
        logger.info("🛑 Kafka Consumer 중지")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'producer' in locals():
            producer.close()

def run_kafka_consumer_sync():
    """Kafka Consumer 동기 실행 (프로세스용)"""
    asyncio.run(run_kafka_consumer())

if __name__ == "__main__":
    # HTTP API 전용 모드로 실행
    logger.info("🌐 HTTP API 전용 모드로 실행")
    port = int(os.getenv("PORT", 8000))
    workers = int(os.getenv("WORKERS", 1))

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        workers=workers,
        reload=False,
        log_level="info"
    )

