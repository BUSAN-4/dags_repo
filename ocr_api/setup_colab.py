#!/usr/bin/env python3
"""
Google Colab 환경 설정 스크립트
Paddle OCR 서비스를 Colab에서 실행하기 위한 자동 설정
"""

import os
import sys
import subprocess
import urllib.request
from pathlib import Path

def run_command(cmd, desc=""):
    """명령어 실행 및 결과 출력"""
    print(f"🔄 {desc}")
    try:
        result = subprocess.run(cmd, shell=True, check=True,
                              capture_output=True, text=True)
        print(f"✅ {desc} 완료")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} 실패: {e}")
        print(f"상세: {e.stderr}")
        return False

def install_packages():
    """필수 패키지 설치"""
    print("📦 패키지 설치 시작...")

    packages = [
        "paddlepaddle-gpu==2.6.2",
        "paddleocr==2.7.0.3",
        "ultralytics==8.3.232",
        "fastapi",
        "uvicorn[standard]",
        "python-multipart",
        "pyngrok",
        "prometheus-client",
        "opencv-python-headless==4.8.1.78",
        "Pillow==10.0.1",
        "numpy==1.24.3",
        "sqlalchemy",
        "pymysql",
        "cryptography",
        "kafka-python",
        "pydantic",
        "httpx",
        "aiofiles",
        "python-dotenv"
    ]

    for package in packages:
        if not run_command(f"pip install {package}", f"패키지 설치: {package}"):
            return False

    return True

def create_directories():
    """필요한 디렉토리 생성"""
    dirs = [
        "car_detect_model",
        "plate_model",
        "car_plate_number_model_inference_data",
        "app",
        "logs"
    ]

    for dir_name in dirs:
        Path(dir_name).mkdir(exist_ok=True)
        print(f"📁 디렉토리 생성: {dir_name}")

    return True

def download_models():
    """모델 파일 다운로드"""
    print("🤖 모델 파일 다운로드 시작...")

    # 모델 URL들 (실제 GitHub URL로 변경 필요)
    models = {
        "car_detect_model/yolov8m.pt": "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8m.pt",
        "plate_model/best.pt": "https://example.com/models/plate_model/best.pt",  # 실제 URL로 변경
    }

    ocr_files = [
        "inference.pdiparams",
        "inference.pdiparams.info",
        "inference.pdmodel",
        "inference.yml"
    ]

    for ocr_file in ocr_files:
        models[f"car_plate_number_model_inference_data/{ocr_file}"] = f"https://example.com/models/ocr/{ocr_file}"  # 실제 URL로 변경

    success_count = 0
    for local_path, url in models.items():
        try:
            print(f"📥 다운로드: {local_path}")
            urllib.request.urlretrieve(url, local_path)
            success_count += 1
            print(f"✅ 다운로드 완료: {local_path}")
        except Exception as e:
            print(f"⚠️  다운로드 실패: {local_path} - {e}")
            print("   수동으로 파일을 업로드해주세요."

    print(f"📊 모델 다운로드 완료: {success_count}/{len(models)}개")
    return success_count > 0

def setup_ngrok():
    """ngrok 설정"""
    print("🌐 ngrok 설정 확인...")

    # pyngrok 설치 확인
    try:
        import pyngrok
        print("✅ pyngrok 설치 확인")
    except ImportError:
        print("❌ pyngrok가 설치되지 않았습니다")
        return False

    # 설정 파일 생성
    config_dir = Path.home() / ".ngrok2"
    config_dir.mkdir(exist_ok=True)

    config_file = config_dir / "ngrok.yml"
    if not config_file.exists():
        config_content = """# ngrok 설정 파일
authtoken: YOUR_TOKEN_HERE
region: ap
console_ui: false
"""
        config_file.write_text(config_content)
        print(f"📝 ngrok 설정 파일 생성: {config_file}")
        print("   authtoken을 실제 토큰으로 변경해주세요")

    return True

def create_startup_script():
    """Colab 시작 스크립트 생성"""
    startup_script = """#!/bin/bash
# Paddle OCR Colab 시작 스크립트

echo "🚀 Paddle OCR Colab 서비스 시작"

# GPU 확인
if command -v nvidia-smi &> /dev/null; then
    echo "🎮 GPU 감지됨:"
    nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader,nounits
else
    echo "⚠️  GPU를 사용할 수 없습니다"
fi

# 메모리 확인
echo "🧠 메모리 상태:"
free -h

# Python 버전 확인
echo "🐍 Python 버전:"
python --version

echo "🔗 서비스 실행 준비 완료"
echo "   다음 명령어로 서버를 시작하세요:"
echo "   export MODE=ngrok"
echo "   export NGROK_AUTHTOKEN=your_token"
echo "   python app/main.py"
"""

    with open("start_colab.sh", "w") as f:
        f.write(startup_script)

    # 실행 권한 부여
    os.chmod("start_colab.sh", 0o755)
    print("📜 시작 스크립트 생성: start_colab.sh")

    return True

def verify_setup():
    """설정 검증"""
    print("🔍 설정 검증 시작...")

    checks = [
        ("Python 버전", "python --version"),
        ("GPU 사용 가능", "python -c \"import torch; print('CUDA:', torch.cuda.is_available())\""),
        ("PaddlePaddle", "python -c \"import paddle; print('Paddle version:', paddle.__version__)\""),
        ("PaddleOCR", "python -c \"from paddleocr import PaddleOCR; print('OCR ready')\""),
        ("YOLO", "python -c \"from ultralytics import YOLO; print('YOLO ready')\""),
        ("FastAPI", "python -c \"import fastapi; print('FastAPI ready')\""),
        ("ngrok", "python -c \"from pyngrok import ngrok; print('ngrok ready')\""),
    ]

    passed = 0
    for name, cmd in checks:
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"✅ {name}: OK")
                passed += 1
            else:
                print(f"❌ {name}: 실패")
                print(f"   {result.stderr.strip()}")
        except Exception as e:
            print(f"❌ {name}: 오류 - {e}")

    print(f"\\n📊 검증 결과: {passed}/{len(checks)}개 통과")

    if passed == len(checks):
        print("🎉 모든 설정이 완료되었습니다!")
        return True
    else:
        print("⚠️  일부 설정이 실패했습니다. 수동으로 확인해주세요.")
        return False

def main():
    """메인 설정 함수"""
    print("🚀 Google Colab용 Paddle OCR 설정 시작")
    print("=" * 50)

    steps = [
        ("패키지 설치", install_packages),
        ("디렉토리 생성", create_directories),
        ("모델 다운로드", download_models),
        ("ngrok 설정", setup_ngrok),
        ("시작 스크립트 생성", create_startup_script),
        ("설정 검증", verify_setup),
    ]

    success_count = 0
    for step_name, step_func in steps:
        print(f"\\n📋 단계: {step_name}")
        if step_func():
            success_count += 1
        else:
            print(f"⚠️  {step_name} 단계 실패")

    print("\\n" + "=" * 50)
    print(f"🎯 설정 완료: {success_count}/{len(steps)} 단계 성공")

    if success_count >= len(steps) - 1:  # 검증 실패는 허용
        print("\\n🎉 Colab 환경 설정이 완료되었습니다!")
        print("\\n📖 사용법:")
        print("1. Google Drive 마운트 (선택): from google.colab import drive; drive.mount('/content/drive')")
        print("2. ngrok 토큰 설정: export NGROK_AUTHTOKEN=your_token")
        print("3. 서버 시작: python app/main.py")
        print("4. 또는 노트북 사용: paddle_ocr_colab.ipynb")
    else:
        print("\\n❌ 설정에 문제가 있습니다. 로그를 확인해주세요.")

if __name__ == "__main__":
    main()
