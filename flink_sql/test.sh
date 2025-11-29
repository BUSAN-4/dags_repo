# flink-sql-client 파드에서 수행
# /opt/flink/dags_repo/flink_sql

# SQL 제출 방법
sql-client.sh gateway -e sql-gateway-service-119.flink.svc.cluster.local:8083 -f test.sql