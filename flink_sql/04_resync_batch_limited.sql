-- ================================================
-- Flink SQL: 기존 RDS 데이터 재전송 (5개씩 순차)
-- ================================================
-- 실행 모드: Batch (Airflow 스케줄링)
-- 용도: 기존 RDS 테이블의 데이터를 5개씩 순차적으로 Kafka로 재전송
-- Airflow 파라미터: :offset (0, 5, 10, 15, 20, ...)
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'resync-batch-limited';

-- ================================================
-- 1. RDS 소스 테이블 (JDBC) - PRIMARY KEY 유지
-- ================================================

-- 1) 사용자 차량 정보
CREATE TABLE IF NOT EXISTS rds_uservehicle (
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
CREATE TABLE IF NOT EXISTS rds_driving_session (
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
CREATE TABLE IF NOT EXISTS rds_driving_session_info (
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
CREATE TABLE IF NOT EXISTS rds_drowsy_drive (
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
CREATE TABLE IF NOT EXISTS rds_arrears_detection (
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
CREATE TABLE IF NOT EXISTS rds_arrears_info (
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
CREATE TABLE IF NOT EXISTS rds_missing_person_detection (
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

-- ================================================
-- 2. Kafka 싱크 테이블 - PRIMARY KEY 제거!
-- ================================================

-- 1) 사용자 차량 정보
CREATE TABLE IF NOT EXISTS kafka_uservehicle (
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
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'uservehicle',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 2) 운행 세션
CREATE TABLE IF NOT EXISTS kafka_driving_session (
    session_id VARCHAR(255),
    car_id VARCHAR(255),
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'driving_session',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 3) 운행 세션 정보
CREATE TABLE IF NOT EXISTS kafka_driving_session_info (
    info_id VARCHAR(255),
    session_id VARCHAR(255),
    dt TIMESTAMP(3),
    roadname VARCHAR(50),
    treveltime DOUBLE,
    `Hour` INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'driving_session_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 4) 졸음 운전 감지
CREATE TABLE IF NOT EXISTS kafka_drowsy_drive (
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
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'drowsy_drive',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 5) 체납 차량 탐지
CREATE TABLE IF NOT EXISTS kafka_arrears_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    car_plate_number VARCHAR(20),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'arrears_detection',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 6) 체납 차량 정보
CREATE TABLE IF NOT EXISTS kafka_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent BOOLEAN,
    updated_at TIMESTAMP(3),
    notice_count INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'arrears_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 7) 실종자 탐지
CREATE TABLE IF NOT EXISTS kafka_missing_person_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    missing_id VARCHAR(64),
    detection_success BOOLEAN,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'missing_person_detection',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ================================================
-- 3. 데이터 적재 (ROW_NUMBER로 5개씩 순차 전송)
-- ================================================
-- Airflow에서 :offset, :offset_end 파라미터 주입
-- 예: :offset = 0, :offset_end = 5 → rn > 0 AND rn <= 5
-- ================================================

-- 사용자 차량 정보
INSERT INTO kafka_uservehicle 
SELECT car_id, age, user_sex, user_location, user_car_class, user_car_brand, user_car_year, user_car_model, user_car_weight, user_car_displace, user_car_efficiency, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY car_id) as rn
    FROM rds_uservehicle
) WHERE rn > :offset AND rn <= :offset_end;

-- 운행 세션
INSERT INTO kafka_driving_session 
SELECT session_id, car_id, start_time, end_time, created_at, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY session_id) as rn
    FROM rds_driving_session
) WHERE rn > :offset AND rn <= :offset_end;

-- 운행 세션 정보
INSERT INTO kafka_driving_session_info 
SELECT info_id, session_id, dt, roadname, treveltime, `Hour`
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY info_id) as rn
    FROM rds_driving_session_info
) WHERE rn > :offset AND rn <= :offset_end;

-- 졸음 운전 감지
INSERT INTO kafka_drowsy_drive 
SELECT drowsy_id, session_id, detected_lat, detected_lon, detected_at, duration_sec, gaze_closure, head_drop, yawn_flag, abnormal_flag, created_at, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY drowsy_id) as rn
    FROM rds_drowsy_drive
) WHERE rn > :offset AND rn <= :offset_end;

-- 체납 차량 탐지
INSERT INTO kafka_arrears_detection 
SELECT detection_id, image_id, car_plate_number, detection_success, detected_lat, detected_lon, detected_time
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY detection_id) as rn
    FROM rds_arrears_detection
) WHERE rn > :offset AND rn <= :offset_end;

-- 체납 차량 정보
INSERT INTO kafka_arrears_info 
SELECT 
    car_plate_number, 
    arrears_user_id, 
    total_arrears_amount, 
    arrears_period, 
    notice_sent, 
    updated_at, 
    notice_count
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY car_plate_number) as rn
    FROM rds_arrears_info
) WHERE rn > :offset AND rn <= :offset_end;

-- 실종자 탐지
INSERT INTO kafka_missing_person_detection 
SELECT detection_id, image_id, missing_id, detection_success, detected_lat, detected_lon, detected_time
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY detection_id) as rn
    FROM rds_missing_person_detection
) WHERE rn > :offset AND rn <= :offset_end;






