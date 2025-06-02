from machine import Pin, I2C
from sensor import BH1750, DPS310, RPR0521rs, SCD41
import time

i2c = I2C(0, scl=Pin(19), sda=Pin(18))

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
pressure_sensor.configure()      # 设置 OSR / 连续测量，并刷新 scale‑factor
time.sleep(0.05)                 # 等待配置生效
rpr_sensor = RPR0521rs(i2c, addr=0x38)
scd_sensor = SCD41(i2c, addr=0x62)
scd_sensor.stop()
time.sleep(1)
scd_sensor.configure()
time.sleep(2)

Factors = pressure_sensor.read_calibration_coefficients()  # 校准系数只依赖芯片，保留

while True:
    print("\n===== Environmental Sensor Data =====")

    # BH1750 Light
    lux_bh = light_sensor.read()

    # DPS310 Pressure & Temperature
    # 连续测量模式下无需再触发单次测量
    scaled_temp, temp_dps = pressure_sensor.read_scaled_temperature(Factors)
    pressure = pressure_sensor.read(scaled_temp, Factors)

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
    print("==============================")

    time.sleep(15)
