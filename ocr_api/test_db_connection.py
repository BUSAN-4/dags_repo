#!/usr/bin/env python3
"""
DB 연결 테스트 스크립트
로컬 DB 연결 상태를 확인하고 필요한 경우 데이터베이스와 테이블을 생성합니다.
"""

import os

# DB 연결 라이브러리 import
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    print('✅ SQLAlchemy 임포트 성공')

    # 현재 설정된 DB 정보 확인
    db_host = os.getenv('DB_HOST', '172.16.11.114')
    db_port = os.getenv('DB_PORT', '3307')
    db_user = os.getenv('DB_USER', 'root')
    db_password = os.getenv('DB_PASSWORD', '0000')
    db_name = os.getenv('DB_NAME', 'busan_db')

    print('🔍 DB 연결 정보:')
    print(f'   호스트: {db_host}:{db_port}')
    print(f'   데이터베이스: {db_name}')
    print(f'   사용자: {db_user}')

    # 연결 테스트 (데이터베이스 없이)
    print('\n🔄 DB 서버 연결 테스트 중...')
    try:
        # 먼저 데이터베이스 없이 연결해서 서버 접속 확인
        server_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}'
        engine = create_engine(server_url, pool_pre_ping=True, pool_recycle=300)
        with engine.connect() as conn:
            result = conn.execute(text('SELECT 1 as test'))
            row = result.fetchone()
            if row and row[0] == 1:
                print('✅ DB 서버 연결 성공!')

                # 데이터베이스 생성 시도
                try:
                    conn.execute(text(f'CREATE DATABASE IF NOT EXISTS {db_name}'))
                    print(f'✅ 데이터베이스 생성: {db_name}')
                except Exception as e:
                    print(f'⚠️  데이터베이스 생성 실패: {e}')

                conn.close()

        # 데이터베이스가 있는 상태로 재연결
        db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=300)
        with engine.connect() as conn:
            result = conn.execute(text('SELECT 1 as test'))
            row = result.fetchone()
            if row and row[0] == 1:
                print('✅ 데이터베이스 연결 성공!')

                # 테이블 존재 확인 및 생성
                result = conn.execute(text("SHOW TABLES LIKE 'arrears_info'"))
                if result.fetchone():
                    print('✅ arrears_info 테이블 존재')

                    # 데이터 개수 확인
                    result = conn.execute(text('SELECT COUNT(*) FROM arrears_info'))
                    count = result.fetchone()[0]
                    print(f'✅ 체납자 데이터: {count}건')

                    if count > 0:
                        # 샘플 데이터 확인
                        result = conn.execute(text('SELECT car_plate_number, total_arrears_amount FROM arrears_info LIMIT 3'))
                        print('📋 샘플 데이터:')
                        for row in result:
                            print(f'   - {row[0]}: {row[1]:,}원')
                    else:
                        print('⚠️  체납자 데이터 없음 - 샘플 데이터 삽입 필요')
                        print('💡 다음 SQL 실행:')
                        print('   INSERT INTO arrears_info (car_plate_number, arrears_user_id, total_arrears_amount) VALUES')
                        print("   ('12가3456', 'user001', 150000),")
                        print("   ('34나5678', 'user002', 200000);")
                else:
                    print('⚠️  arrears_info 테이블 없음 - 테이블 생성 중...')

                    # 테이블 생성
                    create_table_sql = """
                    CREATE TABLE arrears_info (
                        car_plate_number VARCHAR(20) PRIMARY KEY,
                        arrears_user_id VARCHAR(50),
                        total_arrears_amount INT,
                        arrears_period VARCHAR(100),
                        notice_sent BOOLEAN DEFAULT FALSE,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                    conn.execute(text(create_table_sql))
                    print('✅ arrears_info 테이블 생성 완료')

                    # 샘플 데이터 삽입
                    sample_data = [
                        ("12가3456", "user001", 150000, "2024-01", False),
                        ("34나5678", "user002", 200000, "2024-01", True),
                        ("56다7890", "user003", 50000, "2024-01", False)
                    ]

                    for plate, user_id, amount, period, sent in sample_data:
                        conn.execute(text("""
                            INSERT INTO arrears_info
                            (car_plate_number, arrears_user_id, total_arrears_amount, arrears_period, notice_sent)
                            VALUES (:plate, :user_id, :amount, :period, :sent)
                        """), {
                            'plate': plate,
                            'user_id': user_id,
                            'amount': amount,
                            'period': period,
                            'sent': sent
                        })

                    conn.commit()
                    print('✅ 샘플 데이터 3건 삽입 완료')

                    # 삽입된 데이터 확인
                    result = conn.execute(text('SELECT COUNT(*) FROM arrears_info'))
                    count = result.fetchone()[0]
                    print(f'✅ 최종 데이터 개수: {count}건')

        engine.dispose()  # 연결 정리

    except Exception as e:
        print(f'❌ DB 연결 실패: {e}')
        print('💡 가능한 원인:')
        print('   - DB 서버가 실행되지 않음')
        print('   - 네트워크 연결 문제 (방화벽, 포트)')
        print('   - 인증 정보 오류')
        print('   - MySQL/MariaDB가 설치되지 않음')

except ImportError as e:
    print(f'❌ SQLAlchemy 설치 필요: {e}')
    print('💡 다음 명령어로 설치:')
    print('   pip install sqlalchemy pymysql cryptography')

print('\n' + '='*60)
print('🎯 테스트 결과 요약:')
print('1. DB 서버 연결 성공 → 체납자 조회 기능 활성화')
print('2. 테이블/데이터 자동 생성 → API 테스트 준비 완료')
print('3. 연결 실패 → 네트워크/DB 설정 확인 필요')
print('='*60)