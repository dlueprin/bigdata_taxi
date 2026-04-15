from kafka import KafkaConsumer
import json

KAFKA_SERVER = "localhost:9092"
TOPIC = "taxi_data"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset="earliest",
    consumer_timeout_ms=20000,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("正在监听数据...")

for msg in consumer:
    raw = msg.value
    # 把字段名前后的空格都去掉，解决CSV表头带空格的问题
    raw_clean = {k.strip(): v for k, v in raw.items()}

    print("\n【原始数据】")
    print(raw_clean)

    # 数据清洗（现在能正常取到值了）
    cleaned = {
        "medallion": raw_clean.get("medallion", ""),
        "pickup_datetime": raw_clean.get("pickup_datetime", ""),
        "fare_amount": round(float(raw_clean.get("fare_amount", 0)), 2),
        "total_amount": round(float(raw_clean.get("total_amount", 0)), 2),
        "payment_type": raw_clean.get("payment_type", "")
    }

    print("【清洗后】")
    print(cleaned)
