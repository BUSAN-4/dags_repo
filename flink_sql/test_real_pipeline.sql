-- ================================================
-- 실제 파이프라인 테스트: uservehicle → Kafka → test_target
-- ================================================
-- 실행: sql-client.sh gateway -e sql-gateway-service-20.flink.svc.cluster.local:8083 -f test_real_pipeline.sql
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'test-real-pipeline';

-- ================================================
-- STEP 1: RDS 소스 테이블 (실제 uservehicle)
-- ================================================

CREATE TABLE rds_uservehicle_source (
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
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car',
    'table-name' = 'uservehicle',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- ================================================
-- STEP 2: Kafka 중간 테이블
-- ================================================

CREATE TABLE kafka_uservehicle_test (
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
    'topic' = 'test_uservehicle_pipeline',
    'properties.bootstrap.servers' = 'kafka-cluster-kafka-bootstrap.kafka-kubernetes-operator.svc.cluster.local:9092',
    'properties.group.id' = 'test-pipeline-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ================================================
-- STEP 3: RDS 타겟 테이블 (테스트용 수신 테이블)
-- ================================================

CREATE TABLE rds_uservehicle_target (
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
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306/busan_car',
    'table-name' = 'test_uservehicle_target',
    'username' = 'root',
    'password' = 'busan!234pw',
    'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- ================================================
-- STEP 4: 데이터 흐름 실행 (1개 행만!)
-- ================================================

BEGIN STATEMENT SET;

-- uservehicle → Kafka (1개 행)
INSERT INTO kafka_uservehicle_test 
SELECT * FROM rds_uservehicle_source 
LIMIT 1;

-- Kafka → test_target (1개 행)
INSERT INTO rds_uservehicle_target 
SELECT * FROM kafka_uservehicle_test;

END;

