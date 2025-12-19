#!/usr/bin/env python3
"""
Google Colab용 Paddle OCR 빠른 실행 스크립트
ngrok 터널링과 함께 서버를 자동으로 시작합니다.
"""

import os
import sys
import time
import subprocess
from pathlib import Path

def check_environment():
    """Colab 환경 확인"""
    try:
        import google.colab
        print("✅ Google Colab 환경 감지")
        return True
    except ImportError:
        print("⚠️  Google Colab 환경이 아닙니다")
        return False

def setup_ngrok_token():
    """ngrok 토큰 설정"""
    token = os.getenv("NGROK_AUTHTOKEN") or "YOUR_NGROK_TOKEN_HERE"

    if token == "YOUR_NGROK_TOKEN_HERE":
        print("⚠️  ngrok 토큰이 설정되지 않았습니다")
        print("   다음 방법 중 하나를 선택하세요:")
        print("   1. 환경변수 설정: os.environ['NGROK_AUTHTOKEN'] = 'your_token'")
        print("   2. 아래에 직접 입력: ")
        token = input("   ngrok 토큰을 입력하세요: ").strip()

    if token and token != "YOUR_NGROK_TOKEN_HERE":
        os.environ['NGROK_AUTHTOKEN'] = token
        print("✅ ngrok 토큰 설정 완료")
        return True
    else:
        print("❌ ngrok 토큰이 필요합니다 (무료 티어로 계속 진행)")
        return False

def check_gpu():
    """GPU 사용 가능 여부 확인"""
    try:
        result = subprocess.run([
            sys.executable, "-c",
            "import torch; print('GPU:', torch.cuda.is_available())"
        ], capture_output=True, text=True)

        if "GPU: True" in result.stdout:
            print("🎮 GPU 사용 가능")
            return True
        else:
            print("⚠️  GPU 사용 불가능 (CPU 모드로 실행)")
            return False
    except:
        print("⚠️  GPU 확인 실패")
        return False

def check_models():
    """모델 파일 존재 여부 확인"""
    required_files = [
        "car_detect_model/yolov8m.pt",
        "plate_model/best.pt",
        "car_plate_number_model_inference_data/inference.pdmodel",
        "app/main.py"
    ]

    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)

    if missing_files:
        print("⚠️  다음 파일들이 없습니다:")
        for file in missing_files:
            print(f"   - {file}")
        print("\\n🔄 setup_colab.py를 실행하여 모델을 다운로드하세요")
        return False

    print("✅ 모든 모델 파일 존재")
    return True

def start_server():
    """서버 시작"""
    print("🚀 Paddle OCR 서버 시작...")

    # 환경변수 설정
    env = os.environ.copy()
    env.update({
        'MODE': 'ngrok',
        'PORT': '8000',
        'PYTHONPATH': '/content'
    })

    try:
        # 서버 프로세스 시작
        process = subprocess.Popen(
            [sys.executable, 'app/main.py'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        print("⏳ 서버 시작 대기 중...")
        time.sleep(5)

        # ngrok URL 확인
        try:
            import requests
            response = requests.get('http://localhost:4040/api/tunnels', timeout=5)
            if response.status_code == 200:
                tunnels = response.json()['tunnels']
                for tunnel in tunnels:
                    if tunnel['proto'] == 'https':
                        public_url = tunnel['public_url']
                        print("\\n🎉 서버 시작 성공!")
                        print(f"🌐 공개 URL: {public_url}")
                        print(f"🔗 로컬 URL: http://localhost:8000")
                        print(f"📖 API 문서: {public_url}/docs")
                        print("\\n💡 서버 중지: Ctrl+C 또는 런타임 > 실행 중단")
                        break
            else:
                print("⚠️  ngrok 터널 정보를 가져올 수 없습니다")
                print("   http://localhost:4040 에서 직접 확인하세요")
        except Exception as e:
            print(f"⚠️  ngrok 확인 실패: {e}")
            print("   서버는 실행 중일 수 있습니다")

        # 서버 로그 모니터링
        print("\\n📋 서버 로그:")
        try:
            while True:
                if process.poll() is not None:
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\\n🛑 서버 중지 요청")
        finally:
            process.terminate()
            process.wait()
            print("✅ 서버 중지 완료")

    except Exception as e:
        print(f"❌ 서버 시작 실패: {e}")
        return False

    return True

def main():
    """메인 실행 함수"""
    print("🚗 Paddle OCR Colab 런처")
    print("=" * 40)

    # 환경 확인
    if not check_environment():
        print("⚠️  Colab 환경이 아닌 것 같습니다")

    # GPU 확인
    check_gpu()

    # 모델 확인
    if not check_models():
        print("❌ 모델 파일이 없습니다")
        return

    # ngrok 토큰 설정
    setup_ngrok_token()

    print("\\n" + "=" * 40)
    print("🎯 서버 시작 준비 완료")
    print("=" * 40)

    # 서버 시작
    start_server()

if __name__ == "__main__":
    main()
