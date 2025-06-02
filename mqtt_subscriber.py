import paho.mqtt.client as mqtt

# 设置订阅参数
BROKER = "150.65.230.59"  # 或者 mqtt.jaist.ac.jp
PORT = 1883
STUDENT_ID = "s2410083"
TOPIC = f"i483/sensors/{STUDENT_ID}/#"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker.")
        client.subscribe(TOPIC)
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, message):
    print(f"[{message.topic}] → {message.payload.decode()}")

client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, keepalive=60)
client.loop_forever()