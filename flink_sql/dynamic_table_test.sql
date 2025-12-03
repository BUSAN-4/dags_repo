SET 'execution.runtime-mode'='batch';
SET 'sql-client.execution.result-mode'='tableau';
SET 'parallelism.default'='1';
SET 'pipeline.name' = 'mariadb-flink';
CREATE TABLE driving_session(
    `session_id` STRING,
    `car_id` STRING,
    `start_time` TIMESTAMP(3),
    `end_time` TIMESTAMP(3),
    `create_at` TIMESTAMP(3),
    `update_at` TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'default-database' = 'car_db',
    'username' = 'root',
    'password' = 'busan!234pw',
    'url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306'
);

SELECT *
FROM driving_session
LIMIT 10;