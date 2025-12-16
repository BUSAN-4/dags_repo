-- ================================================
-- Flink SQL: busan_car → trash 데이터 전송 (5개씩 순차)
-- ================================================
-- 실행 모드: Batch (Airflow 스케줄링)
-- 용도: busan_car 테이블의 데이터를 5개씩 순차적으로 trash DB로 전송
-- Airflow 파라미터: :offset (0, 5, 10, 15, 20, ...)
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'busan-to-trash-batch';

-- ================================================
-- 1. RDS 소스 테이블 (busan_car DB)
-- ================================================

-- 1) 사용자 차량 정보
CREATE TABLE IF NOT EXISTS source_uservehicle (
    car_id VARCHAR(255),
    age INT,
    user_sex VARCHAR(10),
    user_location VARCHAR(255),
    user_car_class VARCHAR(255),
    user_car_brand VARCHAR(255),
    user_car_year INT,
    user_car_model VARCHAR(255),
    user_car_weight INT,
    user_car_displace INT,
    user_car_efficiency VARCHAR(255),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (car_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'uservehicle',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 2) 운행 세션
CREATE TABLE IF NOT EXISTS source_driving_session (
    session_id VARCHAR(255),
    car_id VARCHAR(255),
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'driving_session',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 3) 운행 세션 정보
CREATE TABLE IF NOT EXISTS source_driving_session_info (
    info_id VARCHAR(255),
    session_id VARCHAR(255),
    dt TIMESTAMP(3),
    roadname VARCHAR(50),
    treveltime DOUBLE,
    `Hour` INT,
    PRIMARY KEY (info_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'driving_session_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 4) 졸음 운전 감지
CREATE TABLE IF NOT EXISTS source_drowsy_drive (
    drowsy_id VARCHAR(255),
    session_id VARCHAR(255),
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_at TIMESTAMP(3),
    duration_sec INT,
    gaze_closure INT,
    head_drop INT,
    yawn_flag INT,
    abnormal_flag INT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (drowsy_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'drowsy_drive',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 5) 체납 차량 탐지
CREATE TABLE IF NOT EXISTS source_arrears_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    car_plate_number VARCHAR(20),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'arrears_detection',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 6) 체납 차량 정보
CREATE TABLE IF NOT EXISTS source_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent BOOLEAN,
    updated_at TIMESTAMP(3),
    notice_count TINYINT,
    PRIMARY KEY (car_plate_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'arrears_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 7) 실종자 탐지
CREATE TABLE IF NOT EXISTS source_missing_person_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    missing_id VARCHAR(64),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'missing_person_detection',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 8) 실종자 정보
CREATE TABLE IF NOT EXISTS source_missing_person_info (
    missing_id VARCHAR(64),
    missing_name VARCHAR(100),
    missing_age INT,
    missing_identity VARCHAR(255),
    registered_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    missing_location VARCHAR(50),
    PRIMARY KEY (missing_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'missing_person_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 9) 차량 외부 이미지
CREATE TABLE IF NOT EXISTS source_vehicle_exterior_image (
    image_id VARCHAR(64),
    session_id VARCHAR(64),
    captured_lat DOUBLE,
    captured_lon DOUBLE,
    captured_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    image_base64 STRING,
    processed BOOLEAN,
    PRIMARY KEY (image_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'vehicle_exterior_image',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- ================================================
-- 2. RDS 싱크 테이블 (trash DB)
-- ================================================

-- 1) 사용자 차량 정보
CREATE TABLE IF NOT EXISTS trash_uservehicle (
    car_id VARCHAR(255),
    age INT,
    user_sex VARCHAR(10),
    user_location VARCHAR(255),
    user_car_class VARCHAR(255),
    user_car_brand VARCHAR(255),
    user_car_year INT,
    user_car_model VARCHAR(255),
    user_car_weight INT,
    user_car_displace INT,
    user_car_efficiency VARCHAR(255),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (car_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'uservehicle',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 2) 운행 세션
CREATE TABLE IF NOT EXISTS trash_driving_session (
    session_id VARCHAR(255),
    car_id VARCHAR(255),
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'driving_session',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 3) 운행 세션 정보
CREATE TABLE IF NOT EXISTS trash_driving_session_info (
    info_id VARCHAR(255),
    session_id VARCHAR(255),
    dt TIMESTAMP(3),
    roadname VARCHAR(50),
    treveltime DOUBLE,
    `Hour` INT,
    PRIMARY KEY (info_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'driving_session_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 4) 졸음 운전 감지
CREATE TABLE IF NOT EXISTS trash_drowsy_drive (
    drowsy_id VARCHAR(255),
    session_id VARCHAR(255),
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_at TIMESTAMP(3),
    duration_sec INT,
    gaze_closure INT,
    head_drop INT,
    yawn_flag INT,
    abnormal_flag INT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (drowsy_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'drowsy_drive',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 5) 체납 차량 탐지
CREATE TABLE IF NOT EXISTS trash_arrears_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    car_plate_number VARCHAR(20),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'arrears_detection',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 6) 체납 차량 정보
CREATE TABLE IF NOT EXISTS trash_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent BOOLEAN,
    updated_at TIMESTAMP(3),
    notice_count TINYINT,
    PRIMARY KEY (car_plate_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'arrears_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 7) 실종자 탐지
CREATE TABLE IF NOT EXISTS trash_missing_person_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    missing_id VARCHAR(64),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'missing_person_detection',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 8) 실종자 정보
CREATE TABLE IF NOT EXISTS trash_missing_person_info (
    missing_id VARCHAR(64),
    missing_name VARCHAR(100),
    missing_age INT,
    missing_identity VARCHAR(255),
    registered_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    missing_location VARCHAR(50),
    PRIMARY KEY (missing_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'missing_person_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 9) 차량 외부 이미지
CREATE TABLE IF NOT EXISTS trash_vehicle_exterior_image (
    image_id VARCHAR(64),
    session_id VARCHAR(64),
    captured_lat DOUBLE,
    captured_lon DOUBLE,
    captured_at TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    image_base64 STRING,
    processed BOOLEAN,
    PRIMARY KEY (image_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/trash?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'vehicle_exterior_image',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- ================================================
-- 3. 데이터 전송 (ROW_NUMBER로 5개씩 순차 전송)
-- ================================================
-- Airflow에서 :offset, :offset_end 파라미터 주입
-- 예: :offset = 0, :offset_end = 5 → rn > 0 AND rn <= 5
-- ================================================

-- 1) 사용자 차량 정보
INSERT INTO trash_uservehicle 
SELECT car_id, age, user_sex, user_location, user_car_class, user_car_brand, user_car_year, user_car_model, user_car_weight, user_car_displace, user_car_efficiency, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY car_id) as rn
    FROM source_uservehicle
) WHERE rn > :offset AND rn <= :offset_end;

-- 2) 운행 세션
INSERT INTO trash_driving_session 
SELECT session_id, car_id, start_time, end_time, created_at, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY session_id) as rn
    FROM source_driving_session
) WHERE rn > :offset AND rn <= :offset_end;

-- 3) 운행 세션 정보
INSERT INTO trash_driving_session_info 
SELECT info_id, session_id, dt, roadname, treveltime, `Hour`
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY info_id) as rn
    FROM source_driving_session_info
) WHERE rn > :offset AND rn <= :offset_end;

-- 4) 졸음 운전 감지
INSERT INTO trash_drowsy_drive 
SELECT drowsy_id, session_id, detected_lat, detected_lon, detected_at, duration_sec, gaze_closure, head_drop, yawn_flag, abnormal_flag, created_at, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY drowsy_id) as rn
    FROM source_drowsy_drive
) WHERE rn > :offset AND rn <= :offset_end;

-- 5) 체납 차량 탐지
INSERT INTO trash_arrears_detection 
SELECT detection_id, image_id, car_plate_number, detection_success, detected_lat, detected_lon, detected_time
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY detection_id) as rn
    FROM source_arrears_detection
) WHERE rn > :offset AND rn <= :offset_end;

-- 6) 체납 차량 정보
INSERT INTO trash_arrears_info 
SELECT car_plate_number, arrears_user_id, total_arrears_amount, arrears_period, notice_sent, updated_at, notice_count
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY car_plate_number) as rn
    FROM source_arrears_info
) WHERE rn > :offset AND rn <= :offset_end;

-- 7) 실종자 탐지
INSERT INTO trash_missing_person_detection 
SELECT detection_id, image_id, missing_id, detection_success, detected_lat, detected_lon, detected_time
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY detection_id) as rn
    FROM source_missing_person_detection
) WHERE rn > :offset AND rn <= :offset_end;

-- 8) 실종자 정보
INSERT INTO trash_missing_person_info 
SELECT missing_id, missing_name, missing_age, missing_identity, registered_at, updated_at, missing_location
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY missing_id) as rn
    FROM source_missing_person_info
) WHERE rn > :offset AND rn <= :offset_end;

-- 9) 차량 외부 이미지
INSERT INTO trash_vehicle_exterior_image 
SELECT image_id, session_id, captured_lat, captured_lon, captured_at, created_at, updated_at, image_base64, processed
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY image_id) as rn
    FROM source_vehicle_exterior_image
) WHERE rn > :offset AND rn <= :offset_end;

