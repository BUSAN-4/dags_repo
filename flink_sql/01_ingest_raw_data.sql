-- ================================================
-- Flink SQL: RDS에서 Kafka로 데이터 적재 (Batch Mode)
-- ================================================
-- 실행 모드: Batch (Airflow 스케줄링)
-- 용도: RDS의 원본 데이터를 시간 범위별로 Kafka에 적재
-- Airflow 파라미터: :start_time, :end_time
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

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
    detection_success TINYINT,
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
    notice_sent TINYINT,
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
    detection_success TINYINT,
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
    detection_success TINYINT,
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
    notice_sent TINYINT,
    updated_at TIMESTAMP(3),
    notice_count TINYINT
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
    detection_success TINYINT,
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
-- 3. 데이터 적재 (시간 범위 필터링)
-- ================================================
-- Airflow에서 :start_time, :end_time 파라미터 주입
-- 예: :start_time = '2024-12-01 00:00:00'
--     :end_time = '2024-12-01 00:01:00'
-- ================================================

-- 사용자 차량 정보
INSERT INTO kafka_uservehicle 
SELECT * FROM rds_uservehicle 
WHERE updated_at >= ':start_time' AND updated_at < ':end_time';

-- 운행 세션
INSERT INTO kafka_driving_session 
SELECT * FROM rds_driving_session 
WHERE updated_at >= ':start_time' AND updated_at < ':end_time';

-- 운행 세션 정보
INSERT INTO kafka_driving_session_info 
SELECT * FROM rds_driving_session_info 
WHERE dt >= ':start_time' AND dt < ':end_time';

-- 졸음 운전 감지
INSERT INTO kafka_drowsy_drive 
SELECT * FROM rds_drowsy_drive 
WHERE updated_at >= ':start_time' AND updated_at < ':end_time';

-- 체납 차량 탐지
INSERT INTO kafka_arrears_detection 
SELECT * FROM rds_arrears_detection 
WHERE detected_time >= ':start_time' AND detected_time < ':end_time';

-- 체납 차량 정보
INSERT INTO kafka_arrears_info 
SELECT * FROM rds_arrears_info 
WHERE updated_at >= ':start_time' AND updated_at < ':end_time';

-- 실종자 탐지
INSERT INTO kafka_missing_person_detection 
SELECT * FROM rds_missing_person_detection 
WHERE detected_time >= ':start_time' AND detected_time < ':end_time';
