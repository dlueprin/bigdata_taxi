import pandas as pd
import time
import json
from kafka import KafkaProducer

CSV_FILE = "trip_data_1.csv"
KAFKA_SERVER = "localhost:9092"
TOPIC = "taxi_data"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(CSV_FILE, nrows=300)
print(f"读取完成，共 {len(df)} 条")

count = 0
for _, row in df.iterrows():
    data = row.to_dict()
    producer.send(TOPIC, value=data)
    count += 1
    print(f"发送第{count}条")
    time.sleep(0.1)

producer.flush()
print("发送完毕！")
