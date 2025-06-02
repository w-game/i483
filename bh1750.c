#include <stdio.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/i2c.h"

#define I2C_MASTER_SCL_IO 19
#define I2C_MASTER_SDA_IO 18
#define I2C_MASTER_NUM I2C_NUM_0
#define I2C_MASTER_FREQ_HZ 100000
#define BH1750_SENSOR_ADDR 0x23
#define BH1750_CMD_CONT_H_RES_MODE 0x10

void i2c_master_init()
{
    i2c_config_t conf = {
        .mode = I2C_MODE_MASTER,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_pullup_en = GPIO_PULLUP_ENABLE,
        .scl_pullup_en = GPIO_PULLUP_ENABLE,
        .master.clk_speed = I2C_MASTER_FREQ_HZ};
    i2c_param_config(I2C_MASTER_NUM, &conf);
    i2c_driver_install(I2C_MASTER_NUM, conf.mode, 0, 0, 0);
}

float bh1750_read_lux()
{
    uint8_t data[2];
    i2c_master_write_read_device(I2C_MASTER_NUM, BH1750_SENSOR_ADDR,
                                 (uint8_t[]){BH1750_CMD_CONT_H_RES_MODE}, 1,
                                 data, 2, pdMS_TO_TICKS(100));
    uint16_t raw = (data[0] << 8) | data[1];
    return raw / 1.2;
}

void app_main()
{
    i2c_master_init();
    while (1)
    {
        float lux = bh1750_read_lux();
        printf("BH1750 Light Intensity: %.2f lux\n", lux);
        vTaskDelay(pdMS_TO_TICKS(3000));
    }
}