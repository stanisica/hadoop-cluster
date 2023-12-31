import subprocess
import time

def run_command(command, work_dir):
    try:
        result = subprocess.run(command, shell=True, check=True, cwd=work_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("success!\n")
    except subprocess.CalledProcessError as e:
        print("error!", e.stderr)

def check_hadoop_connection(work_dir):
    for _ in range(10):
        try:
            result = subprocess.run(
                "docker exec -it namenode hdfs dfsadmin -safemode get", 
                shell=True, cwd=work_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
            )
            if "OFF" in result.stdout:
                print("namenode is ready!\n")
                return True
            else:
                print("waiting for namenode to leave safe mode...")
        except subprocess.CalledProcessError:
            print("waiting for namenode to be ready...")
        time.sleep(2)
    return False

def main():
    print("starting docker compose...")
    run_command("docker-compose up -d", "../docker/")

    if check_hadoop_connection("../docker/"):
        print("running import-data.py script...")
        run_command("python3 import_data.py", "./")

    print("submitting spark job...")
    spark_submit_cmd = "./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./preprocessing.py"
    docker_exec_cmd = f"docker exec -it spark-master {spark_submit_cmd}"
    run_command(docker_exec_cmd, "./")

if __name__ == "__main__":
    main()
