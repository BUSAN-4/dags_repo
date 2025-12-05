-- ================================================
-- Flink SQL: 일일 배치 업데이트 (Daily Batch Update)
-- ================================================
-- 실행 모드: Batch
-- 용도: arrears_info, missing_person_info 하루 단위 갱신
-- 스케줄: 매일 00:00에 실행 (Airflow)
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'daily-info-update';

-- ================================================
-- RDS 소스 테이블 정의
-- ================================================

-- 체납 차량 정보
CREATE TABLE rds_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent TINYINT,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (car_plate_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/car_db',
    'table-name' = 'arrears_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 실종자 정보
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
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/car_db',
    'table-name' = 'missing_person_info',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- ================================================
-- Kafka 싱크 테이블 정의
-- ================================================

CREATE TABLE kafka_arrears_info (
    car_plate_number VARCHAR(20),
    arrears_user_id VARCHAR(64),
    total_arrears_amount INT,
    arrears_period VARCHAR(50),
    notice_sent TINYINT,
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'arrears_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE kafka_missing_person_info (
    missing_id VARCHAR(64),
    missing_name VARCHAR(100),
    missing_age INT,
    missing_identity VARCHAR(255),
    registered_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    missing_location VARCHAR(50)
) WITH (
    'connector' = 'kafka',
    'topic' = 'missing_person_info',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ================================================
-- 데이터 적재 (전체 스냅샷)
-- ================================================
-- 매일 00:00에 전체 데이터를 Kafka로 전송
-- 하루 단위로 갱신되므로 시간 필터 없이 전체 전송
-- ================================================

BEGIN STATEMENT SET;

-- 체납 차량 정보 (전체)
INSERT INTO kafka_arrears_info 
SELECT * FROM rds_arrears_info;

-- 실종자 정보 (전체)
INSERT INTO kafka_missing_person_info 
SELECT * FROM rds_missing_person_info;

END;


