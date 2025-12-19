# 🚗 Paddle OCR 차량 번호판 인식 서비스

Google Colab에서 실행되는 AI 기반 차량 번호판 인식 및 체납자 탐지 서비스

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/username/paddle-ocr/blob/main/paddle_ocr_colab.ipynb)

## ✨ 주요 기능

- **차량 번호판 자동 인식**: YOLOv8 + PaddleOCR을 활용한 고정밀 OCR
- **체납자 차량 탐지**: MariaDB 연동을 통한 실시간 체납자 확인
- **외부 API 제공**: ngrok을 통한 HTTPS API 엔드포인트
- **Kafka 스트리밍 지원**: 실시간 데이터 처리 파이프라인
- **GPU 가속**: Google Colab T4 GPU 활용으로 고성능 처리

## 🚀 빠른 시작 (Google Colab)

### 1. Colab 노트북 열기

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/username/paddle-ocr/blob/main/paddle_ocr_colab.ipynb)

### 2. 자동 설정 실행

```python
# 노트북에서 다음 셀들을 순서대로 실행
!python setup_colab.py  # 환경 설정
!python run_colab.py    # 서버 시작
```

### 3. ngrok 설정

```python
# ngrok 토큰 설정 (무료 티어 가능)
NGROK_TOKEN = "your_ngrok_token_here"
```

## 📁 프로젝트 구조

```
paddle_ocr/
├── app/
│   └── main.py                 # FastAPI 메인 애플리케이션
├── car_detect_model/
│   └── yolov8m.pt             # 차량 검출 YOLO 모델
├── plate_model/
│   └── best.pt                # 번호판 검출 YOLO 모델
├── car_plate_number_model_inference_data/
│   ├── inference.pdmodel      # PaddleOCR 모델
│   ├── inference.pdiparams    # 모델 파라미터
│   └── ...                    # 기타 모델 파일
├── paddle_ocr_colab.ipynb     # Colab 실행 노트북
├── setup_colab.py            # Colab 환경 설정 스크립트
├── run_colab.py              # Colab 서버 런처
├── colab-requirements.txt    # Colab용 패키지 목록
└── README.md                 # 이 파일
```

## 🔧 수동 설치 (Colab 외 환경)

### 필수 패키지 설치

```bash
pip install -r colab-requirements.txt
```

### 모델 파일 준비

```bash
# 모델 파일들을 적절한 위치에 배치
mkdir -p car_detect_model plate_model car_plate_number_model_inference_data
# 모델 파일들을 각 디렉토리에 복사
```

### 서버 실행

```bash
# ngrok 모드로 실행
export MODE=ngrok
export NGROK_AUTHTOKEN=your_token
python app/main.py
```

## 🌐 API 사용법

### 서버가 시작되면 다음 URL들을 사용할 수 있습니다:

- **공개 API**: `https://xxxxx.ngrok.io` (외부 접근용)
- **API 문서**: `https://xxxxx.ngrok.io/docs` (Swagger UI)
- **헬스체크**: `https://xxxxx.ngrok.io/health`

### 주요 엔드포인트

#### 1. 단일 이미지 OCR
```bash
curl -X POST "https://xxxxx.ngrok.io/ocr" \\
  -F "file=@car_image.jpg"
```

#### 2. 배치 이미지 OCR
```bash
curl -X POST "https://xxxxx.ngrok.io/ocr/batch" \\
  -H "Content-Type: application/json" \\
  -d '[{"image_id": "img1", "image_base64": "base64_data"}]'
```

### 응답 형식

```json
{
  "results": [
    {
      "vehicle_id": 1,
      "plate_id": 1,
      "license_plate": "12가3456",
      "confidence": 0.95,
      "yolo_confidence": 0.87,
      "detected_at": "2024-01-01 12:00:00"
    }
  ],
  "arrears_detections": {
    "detections": [
      {
        "license_plate": "12가3456",
        "arrears_info": {
          "total_arrears_amount": 150000,
          "arrears_period": "2023-12",
          "notice_sent": false
        },
        "notification_required": true
      }
    ],
    "total_arrears_found": 1
  },
  "total_vehicles": 1,
  "total_plates": 1,
  "processing_time": 2.34,
  "status": "success"
}
```

## ⚙️ 환경변수 설정

| 변수 | 설명 | 기본값 | 필수 |
|------|------|--------|------|
| `MODE` | 실행 모드 (http/kafka/ngrok/dual) | `http` | ❌ |
| `PORT` | 서버 포트 | `8000` | ❌ |
| `NGROK_AUTHTOKEN` | ngrok 인증 토큰 | - | ⚠️ (ngrok 모드 시) |
| `DB_HOST` | MariaDB 호스트 | `busan-maria...` | ❌ |
| `DB_USER` | DB 사용자 | `root` | ❌ |
| `DB_PASSWORD` | DB 비밀번호 | - | ❌ |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 서버 | `localhost:9092` | ❌ |

## 🎮 Google Colab GPU 활용

Colab에서 GPU를 최대한 활용하기 위한 팁:

### 1. GPU 런타임 설정
- `런타임` > `런타임 유형 변경` > `GPU` 선택

### 2. 메모리 최적화
```python
# GPU 메모리 모니터링
!nvidia-smi

# 메모리 정리
import torch
torch.cuda.empty_cache()
```

### 3. 배치 처리
- 한 번에 여러 이미지를 처리하여 GPU 활용도 향상

## 🔍 모니터링

### 서버 상태 확인
```bash
# 헬스체크
curl http://localhost:8000/health

# 메트릭 (Prometheus)
curl http://localhost:8000/metrics
```

### 로그 확인
```python
# Colab에서 서버 로그 확인
!ps aux | grep python
!tail -f /proc/$(pgrep python)/fd/1 2>/dev/null || echo "로그 확인 불가"
```

## 🚨 문제 해결

### 일반적인 문제들

#### 1. 메모리 부족
```
해결: Colab Pro 사용 또는 모델 최적화
```

#### 2. 모델 다운로드 실패
```
해결: 수동 파일 업로드 또는 GitHub URL 수정
```

#### 3. ngrok 연결 실패
```
해결: 인증 토큰 재설정 또는 무료 티어 사용
```

#### 4. GPU 메모리 부족
```
해결: 배치 크기 줄이기 또는 CPU 모드 사용
```

## 🤝 기여하기

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참고하세요.

## 📞 문의

- **이슈**: [GitHub Issues](https://github.com/username/paddle-ocr/issues)
- **이메일**: your-email@example.com

---

**🎉 즐거운 코딩 되세요!**
