import subprocess
import time
import sys
import os

print("1. Starting Big Data Infrastructure (Kafka, Zookeeper, Hadoop)...")
subprocess.run(["docker-compose", "up", "-d"])

if os.path.exists("output_sink/.spark_ready"):
    os.remove("output_sink/.spark_ready")

print("Resetting Kafka topic to clear old data...")
subprocess.run(["docker", "exec", "kafka", "kafka-topics",
    "--bootstrap-server", "localhost:9092",
    "--delete", "--topic", "health-stream"], capture_output=True)
time.sleep(2)
subprocess.run(["docker", "exec", "kafka", "kafka-topics",
    "--bootstrap-server", "localhost:9092",
    "--create", "--topic", "health-stream",
    "--partitions", "1", "--replication-factor", "1"], capture_output=True)
time.sleep(2)
print("Kafka topic reset.")

print("Setting up HDFS directories...")
subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs",
    "-mkdir", "-p", "/swasthya_data/predictions"], capture_output=True)
subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs",
    "-chmod", "777", "/swasthya_data"], capture_output=True)
subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs",
    "-chmod", "777", "/swasthya_data/predictions"], capture_output=True)
print("HDFS directories ready.")

print("2. Starting PySpark Machine Learning Consumer...")
spark_process = subprocess.Popen([sys.executable, "scripts/spark_consumer.py"])

print("Waiting for Spark to finish loading models and JARs...")
while not os.path.exists("output_sink/.spark_ready"):
    time.sleep(2)
print("Spark is ready!")

print("3. Starting Kafka Data Ingestion Stream...")
if os.path.exists("output_sink/.stream_done"):
    os.remove("output_sink/.stream_done")
producer_process = subprocess.Popen([sys.executable, "scripts/kafka_producer.py"])

print("4. Launching Streamlit Dashboard...")
try:
    dashboard_process = subprocess.Popen(["streamlit", "run", "app/dashboard.py"])

    while not os.path.exists("output_sink/.stream_done"):
        time.sleep(2)
    print("\n✅ Data stream complete. Dashboard will continue showing last results.")
    print("   Press Ctrl+C to shut everything down.")

    dashboard_process.wait()
except KeyboardInterrupt:
    print("\n Shutting down the pipeline...")
    spark_process.terminate()
    producer_process.terminate()
    dashboard_process.terminate()
    subprocess.run(["docker-compose", "down"])
    print("All systems gracefully shut down.")