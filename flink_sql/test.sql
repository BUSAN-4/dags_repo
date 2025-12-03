SET execution.runtime-mode=batch;
SET sql-client.execution.result-mode=tableau;
SET 'parallelism.default'='1';
SET 'pipeline.name' = 'mariadb-flink';
CREATE CATALOG mariadb_catalog WITH (
    'type' = 'jdbc',
    'default-database' = 'car_db',
    'username' = 'root',
    'password' = 'busan!234pw',
    'base-url' = 'jdbc:mysql://busan-maria.cf8s8geeaqc9.ap-northeast-2.rds.amazonaws.com:23306'
);
SELECT *
FROM `mariadb_catalog`.car_db.car_plate_info
LIMIT 10;