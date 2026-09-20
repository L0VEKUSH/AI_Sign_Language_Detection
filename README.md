Producer.py:
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

for i in range(10):

    message = {
        "server_id": f"server{i+1}",
        "cpu_usage": 50 + i * 4,
        "memory_usage": 60 + i
    }

    producer.send(
        "server_metrics",
        value=message
    )

    print("Sent:", message)

    time.sleep(1)

producer.flush()
producer.close()
////////
consumer .py:
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="aiops-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Waiting for messages...")

for message in consumer:

    data = message.value

    server = data["server_id"]
    cpu = data["cpu_usage"]
    memory = data["memory_usage"]

    print("\nReceived:")
    print("Server:", server)
    print("CPU:", cpu, "%")
    print("Memory:", memory, "%")

    if cpu > 80:
        print("ALERT: High CPU detected on", server)
  /////
  topic.py:
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic = NewTopic(
    name="server_metrics",
    num_partitions=1,
    replication_factor=1
)

admin.create_topics(new_topics=[topic])

print("Topic created successfully!")

admin.close()
////cmd///
Kafka installation and setup

Open PowerShell and run:

wsl -d Ubuntu

Now copy and run this entire block inside Ubuntu:

# Update Ubuntu and install requirements
sudo apt update
sudo apt install -y openjdk-17-jdk wget python3-venv

# Download Kafka
cd ~
wget https://archive.apache.org/dist/kafka/4.0.0/kafka_2.13-4.0.0.tgz

# Extract Kafka
tar -xzf kafka_2.13-4.0.0.tgz

# Rename Kafka directory
mv kafka_2.13-4.0.0 kafka

# Open Kafka directory
cd ~/kafka

# Generate cluster ID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# Initialize Kafka storage (FIRST SETUP ONLY)
bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties

# Start Kafka server in background
bin/kafka-server-start.sh -daemon config/server.properties

# Wait for Kafka to start
sleep 10

# Verify Kafka broker
bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Create topic
bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic server_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# List topics
bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

# Describe topic
bin/kafka-topics.sh \
  --describe \
  --topic server_metrics \
  --bootstrap-server localhost:9092
  ////////////
  Python environment setup

Run this entire block inside Ubuntu:

# Create project directory
mkdir -p ~/aiops-kafka

# Open project directory
cd ~/aiops-kafka

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install Kafka Python library
pip install kafka-python

# Verify installation
python -c "from kafka import KafkaProducer, KafkaConsumer; print('Kafka Python installed successful
Create producer.py and consumer.py

Run the following commands inside ~/aiops-kafka to create both Python files automatically.


cd ~/aiops-kafka
source .venv/bin/activate

# Create producer.py
cat > producer.py <<'PY'
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

metrics = [
    {"server_id": "server01", "cpu_usage": 85, "memory_usage": 62},
    {"server_id": "server02", "cpu_usage": 45, "memory_usage": 50},
    {"server_id": "server03", "cpu_usage": 91, "memory_usage": 75},
    {"server_id": "server04", "cpu_usage": 60, "memory_usage": 55},
    {"server_id": "server05", "cpu_usage": 95, "memory_usage": 80},
    {"server_id": "server06", "cpu_usage": 35, "memory_usage": 40},
    {"server_id": "server07", "cpu_usage": 70, "memory_usage": 65},
    {"server_id": "server08", "cpu_usage": 88, "memory_usage": 72},
    {"server_id": "server09", "cpu_usage": 55, "memory_usage": 48},
    {"server_id": "server10", "cpu_usage": 92, "memory_usage": 85}
]

try:
    for metric in metrics:
        future = producer.send("server_metrics", value=metric)
        future.get(timeout=10)
        print("Published:", metric, flush=True)
        time.sleep(1)
finally:
    producer.flush()
    producer.close()

print("All 10 messages published successfully.")
PY

# Create consumer.py
cat > consumer.py <<'PY'
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="aiops-practical-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Kafka consumer started. Waiting for messages...\n")

try:
    for message in consumer:
        metric = message.value

        server = metric["server_id"]
        cpu = metric["cpu_usage"]
        memory = metric["memory_usage"]

        print(f"Server: {server}")
        print(f"CPU: {cpu}%")
        print(f"Memory: {memory}%")

        if cpu > 80:
            print(f"ALERT: High CPU detected on {server}")
        else:
            print("Normal")

        print("-" * 40, flush=True)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
PY

echo "Both Python files created successfully."
/////
Run the complete practical

Open two Ubuntu terminals.

Terminal 1 — Consumer
cd ~/aiops-kafka
source .venv/bin/activate
python consumer.py
Terminal 2 — Producer
cd ~/aiops-kafka
source .venv/bin/activate
python producer.py

The consumer will display server metrics and generate alerts whenever CPU usage exceeds 80%.

5. Verify Kafka messages

Run:

cd ~/kafka

bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic server_metrics \
  --from-beginning \
  --max-messages 10
