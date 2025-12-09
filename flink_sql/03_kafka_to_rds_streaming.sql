-- ================================================
-- Flink SQL: Kafka에서 RDS로 실시간 스트리밍
-- ================================================
-- 실행 모드: Streaming (24/7 실시간 처리)
-- 용도: Kafka 토픽의 데이터를 RDS에 실시간 저장
-- 트리거: Kafka 메시지 도착 시 자동 처리
-- ================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'kafka-to-rds-streaming';
SET 'execution.checkpointing.interval' = '60s';
SET 'state.backend' = 'rocksdb';

-- ================================================
-- Kafka 소스 테이블 정의 (실시간 읽기)
-- ================================================

CREATE TABLE kafka_uservehicle (
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
    'connector' = 'kafka',
    'topic' = 'uservehicle',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_driving_session (
    session_id VARCHAR(255),
    car_id VARCHAR(255),
    start_time TIMESTAMP(3),
    end_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (session_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'driving_session',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_driving_session_info (
    info_id VARCHAR(36),
    session_id VARCHAR(255),
    app_lat DOUBLE,
    app_lon DOUBLE,
    app_prev_lat DOUBLE,
    app_prev_lon DOUBLE,
    voltage TINYINT,
    d_door TINYINT,
    p_door TINYINT,
    rd_door TINYINT,
    rp_door TINYINT,
    t_door TINYINT,
    engine_status TINYINT,
    r_engine_status TINYINT,
    stt_alert TINYINT,
    el_status TINYINT,
    detect_shock TINYINT,
    remain_remote TINYINT,
    autodoor_use TINYINT,
    silence_mode TINYINT,
    low_voltage_alert TINYINT,
    low_voltage_engine TINYINT,
    temperature TINYINT,
    app_travel TINYINT,
    app_avg_speed FLOAT,
    app_accel FLOAT,
    app_gradient FLOAT,
    app_rapid_acc INT,
    app_rapid_deacc INT,
    speed FLOAT,
    createdDate TIMESTAMP(3),
    app_weather_status VARCHAR(255),
    app_precipitation FLOAT,
    dt TIMESTAMP(3),
    roadname VARCHAR(50),
    treveltime DOUBLE,
    Hour INT,
    PRIMARY KEY (info_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'driving_session_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_drowsy_drive (
    drowsy_id VARCHAR(64),
    session_id VARCHAR(64),
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
    'connector' = 'kafka',
    'topic' = 'drowsy_drive',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_arrears_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    car_plate_number VARCHAR(20),
    detection_success TINYINT,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'arrears_detection',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_missing_person_detection (
    detection_id VARCHAR(64),
    image_id VARCHAR(64),
    missing_id VARCHAR(64),
    detection_success TINYINT,
    detected_lat DOUBLE,
    detected_lon DOUBLE,
    detected_time TIMESTAMP(3),
    PRIMARY KEY (detection_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'missing_person_detection',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent TINYINT,
    notice_count TINYINT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (car_plate_number) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'arrears_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_missing_person_info (
    missing_id VARCHAR(64),
    missing_name VARCHAR(100),
    missing_age INT,
    missing_identity VARCHAR(255),
    registered_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    missing_location VARCHAR(50),
    PRIMARY KEY (missing_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'missing_person_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ================================================
-- RDS 싱크 테이블 정의 (실시간 쓰기)
-- ================================================

CREATE TABLE rds_uservehicle (
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_driving_session (
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_driving_session_info (
    info_id VARCHAR(36),
    session_id VARCHAR(255),
    app_lat DOUBLE,
    app_lon DOUBLE,
    app_prev_lat DOUBLE,
    app_prev_lon DOUBLE,
    voltage TINYINT,
    d_door TINYINT,
    p_door TINYINT,
    rd_door TINYINT,
    rp_door TINYINT,
    t_door TINYINT,
    engine_status TINYINT,
    r_engine_status TINYINT,
    stt_alert TINYINT,
    el_status TINYINT,
    detect_shock TINYINT,
    remain_remote TINYINT,
    autodoor_use TINYINT,
    silence_mode TINYINT,
    low_voltage_alert TINYINT,
    low_voltage_engine TINYINT,
    temperature TINYINT,
    app_travel TINYINT,
    app_avg_speed FLOAT,
    app_accel FLOAT,
    app_gradient FLOAT,
    app_rapid_acc INT,
    app_rapid_deacc INT,
    speed FLOAT,
    createdDate TIMESTAMP(3),
    app_weather_status VARCHAR(255),
    app_precipitation FLOAT,
    dt TIMESTAMP(3),
    roadname VARCHAR(50),
    treveltime DOUBLE,
    Hour INT,
    PRIMARY KEY (info_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'driving_session_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_drowsy_drive (
    drowsy_id VARCHAR(64),
    session_id VARCHAR(64),
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_arrears_detection (
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_missing_person_detection (
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent TINYINT,
    notice_count TINYINT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (car_plate_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car?characterEncoding=UTF-8&useUnicode=true&serverTimezone=UTC',
    'table-name' = 'arrears_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

CREATE TABLE rds_missing_person_info (
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
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s'
);

-- ================================================
-- 실시간 스트리밍 처리 (Kafka → RDS)
-- ================================================
-- 각 Kafka 토픽의 메시지를 실시간으로 RDS에 저장
-- ================================================

BEGIN STATEMENT SET;

-- 사용자-차량 정보
INSERT INTO rds_uservehicle 
SELECT * FROM kafka_uservehicle;

-- 운행 세션
INSERT INTO rds_driving_session 
SELECT * FROM kafka_driving_session;

-- 운행 상세 정보
INSERT INTO rds_driving_session_info 
SELECT * FROM kafka_driving_session_info;

-- 졸음 운전 감지
INSERT INTO rds_drowsy_drive 
SELECT * FROM kafka_drowsy_drive;

-- 체납 차량 감지
INSERT INTO rds_arrears_detection 
SELECT * FROM kafka_arrears_detection;

-- 실종자 차량 감지
INSERT INTO rds_missing_person_detection 
SELECT * FROM kafka_missing_person_detection;

-- 체납 차량 정보
INSERT INTO rds_arrears_info 
SELECT * FROM kafka_arrears_info;

-- 실종자 정보
INSERT INTO rds_missing_person_info 
SELECT * FROM kafka_missing_person_info;

END;

