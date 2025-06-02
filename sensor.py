import time


class Sensor:
    def __init__(self, i2c, addr=0x23):
        self.i2c = i2c
        self.addr = addr
    
    def configure(self, mode=0x00):
        self.i2c.writeto(self.addr, bytes([mode]))
        
    def read(self):
        pass


class BH1750(Sensor):
    def __init__(self, i2c, addr=0x23):
        super().__init__(i2c, addr)
    
    def read(self):
        self.configure(0x10)  # 高分辨率连续模式
        time.sleep_ms(180)
        data = self.i2c.readfrom(self.addr, 2)
        raw = (data[0] << 8) | data[1]
        return raw / 1.2




# ----- DPS310 Barometric Pressure & Temperature Sensor -----

class DPS310(Sensor):
    """
    Driver for the Infineon DPS310 barometric pressure & temperature sensor.

    * `measure()` returns a tuple: (pressure_in_Pa, temperature_in_°C).
    * The driver operates the device in one‑shot (command) mode with
      oversampling ×1 for both pressure and temperature.
    """
    # ----- Register map constants -----
    _REG_PRS_B2   = 0x00  # raw pressure MSB
    _REG_TMP_B2   = 0x03  # raw temperature MSB
    _REG_PRS_CFG  = 0x06
    _REG_TMP_CFG  = 0x07
    _REG_MEAS_CFG = 0x08
    _REG_CFG      = 0x09
    _REG_RESET    = 0x0C
    _REG_COEF     = 0x10  # 18‑byte calibration coefficient block

    _RESET_BYTE   = 0x89  # soft‑reset magic value

    def __init__(self, i2c, addr=0x77, osr_p=0, osr_t=0):
        super().__init__(i2c, addr)
        self._osr_p = osr_p & 0x07   # oversampling setting (0 → 1× … 7 → 128×)
        self._osr_t = osr_t & 0x07
        # scaling factors according to DPS310 datasheet (Table “Scaling Factors”)
        _SF_P = (524288, 1572864, 3670016, 7864320,
                 253952, 516096, 1040384, 2088960)
        _SF_T = (524288, 1572864, 3670016, 7864320,
                 524288, 1572864, 3670016, 7864320)
        self._scale_p = _SF_P[self._osr_p]
        self._scale_t = _SF_T[self._osr_t]

        self._reset()
        self._configure()
        self._read_calibration()

    # ----- low‑level helpers -----
    def _write(self, reg, val):
        self.i2c.writeto_mem(self.addr, reg, bytes([val]))

    def _read8(self, reg):
        return self.i2c.readfrom_mem(self.addr, reg, 1)[0]

    def _read24(self, reg):
        b2, b1, b0 = self.i2c.readfrom_mem(self.addr, reg, 3)
        raw = (b2 << 16) | (b1 << 8) | b0
        if raw & 0x800000:            # sign‑extend 24‑bit two's complement
            raw -= 1 << 24
        return raw

    def _reset(self):
        """Soft‑reset and wait until coefficients are ready."""
        self._write(self._REG_RESET, self._RESET_BYTE)
        time.sleep_ms(10)
        # Datasheet: wait for both COEF_RDY (bit7) and SENSOR_RDY (bit6)
        for _ in range(100):                    # up to ~500 ms
            status = self._read8(self._REG_MEAS_CFG)
            if (status & 0xC0) == 0xC0:         # both bits set
                break
            time.sleep_ms(5)
        else:
            print("DPS310 reset timeout, MEAS_CFG:", hex(status))

    def _configure(self):
        # oversampling configuration (OSR bits = [6:4])
        self._write(self._REG_PRS_CFG, self._osr_p << 4)          # pressure OSR
        # TMP_EXT = 1 (use external MEMS temperature sensor) + temperature OSR
        self._write(self._REG_TMP_CFG, 0x80 | (self._osr_t << 4))  # use external MEMS temperature sensor
        self._write(self._REG_CFG, 0x00)                          # standby, no FIFO

    @staticmethod
    def _se(val, bits):
        """Sign‑extend *val* assuming it is *bits* wide."""
        sign = 1 << (bits - 1)
        return (val & (sign - 1)) - (val & sign)

    def _read_calibration(self):
        buf = self.i2c.readfrom_mem(self.addr, self._REG_COEF, 18)
        self.c0   = self._se((buf[0] << 4) | (buf[1] >> 4), 12)
        self.c1   = self._se(((buf[1] & 0x0F) << 8) | buf[2], 12)
        self.c00  = self._se((buf[3] << 12) | (buf[4] << 4) | (buf[5] >> 4), 20)
        self.c10  = self._se(((buf[5] & 0x0F) << 16) | (buf[6] << 8) | buf[7], 20)
        self.c01  = self._se((buf[8]  << 8) | buf[9] , 16)
        self.c11  = self._se((buf[10] << 8) | buf[11], 16)
        self.c20  = self._se((buf[12] << 8) | buf[13], 16)
        self.c21  = self._se((buf[14] << 8) | buf[15], 16)
        self.c30  = self._se((buf[16] << 8) | buf[17], 16)
        print("[DPS310 COEF]",
              f"c0={self.c0}, c1={self.c1}, c00={self.c00}, c10={self.c10},",
              f"c01={self.c01}, c11={self.c11}, c20={self.c20},",
              f"c21={self.c21}, c30={self.c30}")

    # ----- measurement helpers -----
    def _wait_ready(self, mask):
        """Wait until the ready bit(s) given by *mask* are set."""
        while (self._read8(self._REG_MEAS_CFG) & mask) == 0:
            time.sleep_ms(2)

    # ----- public API -----
    def measure(self):
        """Return (pressure_Pa, temperature_°C) using one‑shot measurements."""
        # Temperature first
        self._write(self._REG_MEAS_CFG, 0x02)      # command: temperature
        self._wait_ready(0x20)                     # TMP_RDY
        t_raw = self._read24(self._REG_TMP_B2)

        # Pressure
        self._write(self._REG_MEAS_CFG, 0x01)      # command: pressure
        self._wait_ready(0x10)                     # PRS_RDY
        p_raw = self._read24(self._REG_PRS_B2)

        # DEBUG raw values
        # print(f"[DPS310 RAW] t_raw={t_raw}, p_raw={p_raw}")

        t_sc = t_raw / self._scale_t
        p_sc = p_raw / self._scale_p

        # datasheet compensation formula
        pressure = (self.c00
                    + p_sc * (self.c10 + p_sc * (self.c20 + p_sc * self.c30))
                    + t_sc * self.c01
                    + t_sc * p_sc * (self.c11 + p_sc * self.c21))

        temperature = self.c0 / 2 + self.c1 * t_sc

        return pressure, temperature

class RPR0521rs(Sensor):
    """
    Micropython driver for ROHM RPR‑0521RS
    (digital ambient‑light + proximity sensor with integrated IR LED).

    * `read()` returns a tuple: (ps, lux) where
        - `ps`  is the 12‑bit proximity count (0‑4095)
        - `lux` is a rough illuminance value in lux.

    The driver runs the device in continuous ALS+PS mode with default gains
    (ALS ×1, PS ×1) and ~100 ms ALS / 50 ms PS measurement windows.
    """

    # -------- register map --------
    _REG_SYSTEM_CONTROL   = 0x40
    _REG_MODE_CONTROL     = 0x41
    _REG_ALS_PS_CONTROL   = 0x42
    _REG_PS_CONTROL       = 0x43

    _REG_PS_LSB           = 0x44
    _REG_PS_MSB           = 0x45
    _REG_ALS0_LSB         = 0x46
    _REG_ALS0_MSB         = 0x47
    _REG_ALS1_LSB         = 0x48
    _REG_ALS1_MSB         = 0x49
    _REG_MANUFACT_ID      = 0x92   # should read 0xE0
    _LUX_CAL = 2.3   # calibrated scale factor (lux divided by this)

    # -------- constructor --------
    def __init__(self, i2c, addr=0x38):
        super().__init__(i2c, addr)

        # optional sanity check on power‑up
        try:
            part_id = self._read8(self._REG_MANUFACT_ID)
            if part_id != 0xE0:
                print("Warning: unexpected RPR‑0521RS ID:", hex(part_id))
        except Exception as exc:
            print("RPR‑0521RS ID read failed:", exc)

        self.configure()  # put device in continuous ALS+PS mode

    # -------- low‑level helpers --------
    def _write8(self, reg, val):
        self.i2c.writeto_mem(self.addr, reg, bytes([val & 0xFF]))

    def _read8(self, reg):
        return self.i2c.readfrom_mem(self.addr, reg, 1)[0]

    def _read16(self, reg):
        lsb, msb = self.i2c.readfrom_mem(self.addr, reg, 2)
        return (msb << 8) | lsb   # sensor stores LSB first

    # -------- configuration --------
    def configure(self,
                  als_gain=0x01,   # ALS gain ×1
                  ps_gain=0x02,    # PS  gain ×4
                  led_current=0x02 # 50 mA IR LED
                  ):
        """
        Configure sensor for continuous measurement.
        """
        # ALS/PS gains & LED current (ALS_PS_CONTROL bits)
        self._write8(self._REG_ALS_PS_CONTROL,
                     (als_gain << 4) | (als_gain << 2) | led_current)

        # PS gain & interrupt persistence = 1
        self._write8(self._REG_PS_CONTROL,
                     (ps_gain << 4) | 0x01)

        # Enable ALS & PS, normal pulse width, normal mode,
        # measurement‑time code 0b0101 ≈ 100 ms ALS / 50 ms PS
        # measurement‑time code 0b0101 ≈ 100 ms ALS / 50 ms PS
        mode_val = 0xC0 | 0x05     # ALS_EN | PS_EN | meas‑time
        self._write8(self._REG_MODE_CONTROL, mode_val)

    # -------- public API --------
    def read(self):
        """
        Return (proximity_count, lux).

        * `proximity_count` is a 12‑bit value representing the amount
          of IR light reflected back to the sensor (higher ⇒ nearer).
        * `lux` is a coarse lux estimate derived from ALS channels.
        """

        # --- proximity ---
        ps_raw = self._read16(self._REG_PS_LSB) & 0x0FFF  # 12‑bit

        # --- ambient light ---
        ch0 = self._read16(self._REG_ALS0_LSB)   # visible + IR
        ch1 = self._read16(self._REG_ALS1_LSB)   # IR

        # --- lux calculation (ROHM application note) ---
        # Compute channel ratio and select formula segment
        lux = 0
        if ch0:                                    # avoid division by zero
            ratio = ch1 / ch0
            if ratio < 0.45:
                lux = 1.7743 * ch0
            elif ratio < 0.64:
                lux = 4.2785 * ch0 - 1.9548 * ch1
            elif ratio < 0.85:
                lux = 0.5926 * ch0 + 0.1185 * ch1
            # else lux stays 0 (invalid or very IR‑rich light)

            lux /= self._LUX_CAL   # apply calibration factor

        # Debug print — uncomment if needed
        # print(f"[RPR RAW] CH0={ch0}, CH1={ch1}, ratio={ratio:.3f if ch0 else 0}, lux={lux:.2f}")

        # Debug (uncomment if needed)
        # print(f"[RPR RAW] CH0={ch0}, CH1={ch1}, lux_raw={lux_raw:.2f}, lux={lux:.2f}")

        return ps_raw, round(lux, 2)


class SCD41(Sensor):
    def configure(self):
        """启动周期性测量（仅调用一次）"""
        self.i2c.writeto(self.addr, b'\x21\xb1')
        time.sleep(1)
        # self.start()
        time.sleep(1)

    def stop(self):
        self.i2c.writeto(self.addr, b'\x3f\x86')
        time.sleep(1)

    def is_data_ready(self):
        """检查 SCD41 数据是否准备好"""
        self.i2c.writeto(self.addr, b'\xe4\xb8')
        time.sleep_ms(2)
        data = self.i2c.readfrom(self.addr, 3)
        print(f"SCD41 raw ready status: {data}")
        return (data[1] & 0x07) > 0

    def read(self):
        """读取最新 CO2, 温度, 湿度（已启动测量后调用）"""
        self.i2c.writeto(self.addr, b'\xec\x05')
        time.sleep_ms(2)
        data = self.i2c.readfrom(self.addr, 9)

        if len(data) != 9:
            raise ValueError("读取数据长度错误，应为9字节")

        co2 = (data[0] << 8) | data[1]
        temp_raw = (data[3] << 8) | data[4]
        hum_raw = (data[6] << 8) | data[7]

        temperature = -45 + 175 * (temp_raw / 65536)
        humidity = 100 * (hum_raw / 65536)
        return co2, round(temperature, 2), round(humidity, 2)
