from kafka import KafkaConsumer, KafkaProducer
from collections import deque
import json, time

BOOTSTRAP = "150.65.230.59:9092"
SID = "s2410083"

TOP_BH = f"i483-sensors-{SID}-BH1750-illumination"
TOP_CO2 = f"i483-sensors-{SID}-SCD41-co2"

TOP_AVG = f"i483-sensors-{SID}-BH1750_avg-illumination"
TOP_FLAG = f"i483-actuators-{SID}-co2_threshold-crossed"
TOP_FLAG_SENS = f"i483-sensors-{SID}-co2_threshold-crossed"   # for assignment compliance

# --- Kafka clients
consumer = KafkaConsumer(
    TOP_BH, TOP_CO2,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="latest",
    group_id=f"{SID}_proc")

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

# --- rolling buffer for 5 min (20 × 15 s = 300 s)
illum_buf = deque()          # (timestamp, value)
last_flag = None

start_time = time.time()

while True:
    for msg in consumer:
        now = time.time()
        topic = msg.topic
        value = float(msg.value.decode())

        # ---------- 光照 ----------
        if topic == TOP_BH:
            illum_buf.append((now, value))
            # 删除 >5 min 旧值
            while illum_buf and now - illum_buf[0][0] > 300:
                illum_buf.popleft()

            # 每 30 s 取一次平均
            if now - start_time > 30 and illum_buf:
                start_time = now
                avg = sum(v for _, v in illum_buf) / len(illum_buf)
                producer.send(TOP_AVG, f"{avg:.2f}".encode())
                producer.flush()
                print("=> publish avg:", avg)

        # ---------- CO₂ 阈值 ----------
        elif topic == TOP_CO2:
            flag = "yes" if value > 700 else "no"
            if flag != last_flag:
                payload = flag.encode()
                # 发送到 sensors 主题（用于报告）
                producer.send(TOP_FLAG_SENS, payload)
                # 发送到 actuators 主题（供 kktt 回推 MQTT / LED 使用）
                producer.send(TOP_FLAG, payload)
                producer.flush()
                print("=> publish flag:", flag)
                last_flag = flag