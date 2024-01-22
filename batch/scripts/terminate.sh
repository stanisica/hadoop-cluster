docker compose -f ../docker/docker-compose.yaml down -v
# docker rmi docker-superset:latest
# docker rmi bde2020/spark-worker:3.0.1-hadoop3.2
# docker rmi bde2020/spark-master:3.0.1-hadoop3.2
# docker rmi bde2020/hive:2.3.2-postgresql-metastore 
# docker rmi bde2020/hive-metastore-postgresql:2.3.0
# docker rmi bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
# docker rmi bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
echo "y" | docker volume prune
echo "Terminated."