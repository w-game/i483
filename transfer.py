import paho.mqtt.client as mqtt

LOCAL_BROKER = "10.106.112.98"   # your laptop LAN IP
JAIST_BROKER = "150.65.230.59"
JAIST_PORT = 1883
STUDENT_ID = "s2410083"
TOPIC = f"i483/sensors/{STUDENT_ID}/#"

def on_message(client, userdata, message):
    print(f"Forwarding: {message.topic} → {message.payload.decode()}")
    jaist_client.publish(message.topic, message.payload)

# def on_publish(client, userdata, mid):
#     print("sent mid:", mid)


# 本地客户端（接收 ESP32 的数据）
local_client = mqtt.Client(client_id="forwarder_local",
                           protocol=mqtt.MQTTv311)
local_client.connect(LOCAL_BROKER, 1884)
local_client.subscribe(TOPIC)
local_client.on_message = on_message

# 远程客户端（转发到 JAIST）
jaist_client = mqtt.Client(client_id="forwarder_to_jaist",
                           protocol=mqtt.MQTTv311)
jaist_client.connect(JAIST_BROKER, JAIST_PORT)
# jaist_client.on_publish = on_publish
jaist_client.loop_start()   # enable background network handling

# ---- subscribe remote actuator topic and pipe back to local ----
ACT_TOPIC = f"i483/actuators/{STUDENT_ID}/#"

def on_remote(cli, userdata, msg):
    print(f"Backhaul : {msg.topic} ← {msg.payload.decode()}")
    local_client.publish(msg.topic, msg.payload)

jaist_client.on_message = on_remote
jaist_client.subscribe(ACT_TOPIC)

local_client.loop_forever()