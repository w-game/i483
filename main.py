from machine import Pin, I2C
from machine import Timer
from sensor import BH1750, DPS310, RPR0521rs, SCD41
import time
from umqtt.simple import MQTTClient
import network

def connect_wifi(ssid, password):
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    if not wlan.isconnected():
        print(f"Connecting to {ssid}...")
        wlan.connect(ssid, password)
        timeout = 10
        while not wlan.isconnected() and timeout > 0:
            time.sleep(1)
            timeout -= 1
    if wlan.isconnected():
        print("Wi-Fi connected:", wlan.ifconfig())
    else:
        print("Failed to connect.")
    return wlan


# connect_wifi("TP-Link_C0D4", "97404941")
connect_wifi("JAIST-GUEST", "97404941")

LED = Pin(2, Pin.OUT)
LED_ACTIVE_LOW = False
blink_timer = Timer(0)

def _led_write(state: bool):
    """state=True 点亮, False 熄灭"""
    LED.value(0 if (state and LED_ACTIVE_LOW) else
              1 if (state and not LED_ACTIVE_LOW) else
              1 if LED_ACTIVE_LOW else 0)

def led_set(mode):
    """mode = 'on' | 'off' | 'blink' (500 ms)"""
    blink_timer.deinit()
    if mode == "on":
        _led_write(True)
    elif mode == "off":
        _led_write(False)
    elif mode == "blink":
        _led_write(True)
        blink_timer.init(period=500,
                         mode=Timer.PERIODIC,
                         callback=lambda t: LED.value(not LED.value()))

i2c = I2C(0, scl=Pin(21), sda=Pin(19), freq=100_000)

# MQTT 初始化
mqtt_client = MQTTClient("kadai2_s2410083", "10.106.112.98", port=1884)
mqtt_client.connect()
student_id = "s2410083"

# ----- second MQTT client to listen for CO₂ threshold -----
sub_client = MQTTClient("sub_"+student_id, "10.106.112.98", port=1884)
def on_msg(topic, payload):
    print("[MQTT ACT]", topic, payload)  # debug
    if payload == b"yes":
        led_set("blink")
    else:
        led_set("off")
sub_client.set_callback(on_msg)
sub_client.connect()
sub_client.subscribe(b"i483/actuators/%s/#" % student_id.encode())

devices = i2c.scan()

if devices:
    print("Detected I2C devices:")
    for dev in devices:
        print(" - Address: 0x{:02X}".format(dev))
else:
    print("No I2C devices detected")
    
try:
    device_id = i2c.readfrom_mem(0x38, 0x92, 1)[0]
    print("RPR device ID:", hex(device_id))
except Exception as e:
    print("Failed to read device ID:", e)
    
light_sensor = BH1750(i2c, addr=0x23)
pressure_sensor = DPS310(i2c, addr=0x77)
time.sleep(0.05)                 # 等待配置生效
rpr_sensor = RPR0521rs(i2c, addr=0x38)
scd_sensor = SCD41(i2c, addr=0x62)
scd_sensor.stop()
time.sleep(1)
scd_sensor.configure()
time.sleep(2)


while True:
    print("\n===== Environmental Sensor Data =====")

    # BH1750 Light
    lux_bh = light_sensor.read()

    # DPS310 Pressure & Temperature (one‑shot)
    pressure, temp_dps = pressure_sensor.measure()   # pressure in Pa, temp in °C
    print(f"[DPS310 Debug] pressure:{pressure:.1f} Pa, temp:{temp_dps:.2f} °C")

    # RPR0521RS Light + PS
    ps, lux_rpr = rpr_sensor.read()

    # SCD41 CO2 Temperature Humidity
    for _ in range(10):
        if scd_sensor.is_data_ready():
            break
        time.sleep(0.5)
    else:
        print("Warning: SCD41 data not ready, skip")
        continue  # 跳过当前循环

    co2, temp_scd, hum_scd = scd_sensor.read()

    print("\n[Illuminance]")
    print(f"  - BH1750:      {lux_bh:.2f} lux")
    print(f"  - RPR0521RS:   {lux_rpr:.2f} lux  (PS: {ps})")

    print("\n[Pressure]")
    print(f"  - Pressure:    {pressure:.1f} Pa")
    print(f"  - Temperature: {temp_dps:.2f} ℃    (from DPS310)")

    print("\n[Air Quality]")
    print(f"  - CO₂ Level:   {co2} ppm")
    print(f"  - Temperature: {temp_scd:.2f} ℃    (from SCD41)")
    print(f"  - Humidity:    {hum_scd:.2f} %")
    # MQTT 发布
    mqtt_client.publish(f"i483/sensors/{student_id}/BH1750/illumination", str(round(lux_bh, 2)))
    mqtt_client.publish(f"i483/sensors/{student_id}/RPR0521RS/illumination", str(round(lux_rpr, 2)))
    mqtt_client.publish(f"i483/sensors/{student_id}/RPR0521RS/proximity", str(ps))
    mqtt_client.publish(f"i483/sensors/{student_id}/DPS310/temperature", str(round(temp_dps, 2)))
    mqtt_client.publish(f"i483/sensors/{student_id}/DPS310/air_pressure", str(round(pressure, 2)))
    mqtt_client.publish(f"i483/sensors/{student_id}/SCD41/co2", str(co2))
    mqtt_client.publish(f"i483/sensors/{student_id}/SCD41/temperature", str(round(temp_scd, 2)))
    mqtt_client.publish(f"i483/sensors/{student_id}/SCD41/humidity", str(round(hum_scd, 2)))
    print("MQTT publish complete.")
    print("==============================")

    # handle incoming actuator messages
    sub_client.check_msg()     # non‑blocking; will call on_msg() if any
    time.sleep(15)
