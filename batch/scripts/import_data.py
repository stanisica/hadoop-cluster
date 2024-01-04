import subprocess

def put_file_in_hdfs(namenode_container, local_file_path, hdfs_dest_path):
    try:
        create_dir = f"docker exec -it {namenode_container} hdfs dfs -mkdir -p /data"
        subprocess.run(create_dir, check=True, shell=True)

        import_data = f"docker exec -it {namenode_container} hdfs dfs -put {local_file_path} {hdfs_dest_path}"
        subprocess.run(import_data, check=True, shell=True)
        print(f"File {local_file_path} successfully uploaded to /data/{hdfs_dest_path} in HDFS.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

namenode_container_name = "namenode"
local_csv_file = "/batch.csv"
hdfs_destination = "/data/batch.csv"

put_file_in_hdfs(namenode_container_name, local_csv_file, hdfs_destination)
