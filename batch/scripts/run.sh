echo "Starting Docker Compose..."
docker compose -f ../docker/docker-compose.yaml build
docker compose -f ../docker/docker-compose.yaml up -d

check_namenode_ready() {
    while true; do
        if docker exec namenode hdfs dfsadmin -report > /dev/null 2>&1; then
            echo "Namenode is ready."
            break
        else
            echo "Waiting for namenode to be ready..."
            sleep 5
        fi
    done
}

echo "Checking if namenode is ready..."
check_namenode_ready

if docker exec namenode hdfs dfs -put /batch.csv /batch.csv; then
    echo "Files successfully copied to HDFS."
    
    echo "Running spark.py scripts..."
    docker exec -it spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./ctg_views.py
    docker exec -it spark-master ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./event_window.py

else
    echo "Failed to copy files to HDFS."
fi

echo "Script completed."
