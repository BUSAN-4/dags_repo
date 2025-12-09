-- ================================================
-- STEP 2: Kafka → RDS (test_uservehicle_target로 전송)
-- 실행: sql-client.sh gateway -e sql-gateway-service-20.flink.svc.cluster.local:8083 -f test_step2_kafka_to_rds.sql
-- ================================================

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'test-step2-kafka-to-rds';

-- Kafka 소스 테이블 (bounded 읽기)
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
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- RDS 타겟 테이블 (테스트용)
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

-- Kafka → RDS 전송
INSERT INTO rds_uservehicle_target
SELECT *
FROM kafka_uservehicle_test;

