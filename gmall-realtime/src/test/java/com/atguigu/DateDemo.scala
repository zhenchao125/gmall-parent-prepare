package com.atguigu

import java.time.{LocalDateTime, ZoneId}
import java.util.TimeZone

/**
 * Author lzc
 * Date 2020/8/31 3:21 下午
 */
object DateDemo {
    def main(args: Array[String]): Unit = {
        val dateTime: LocalDateTime = LocalDateTime.now()
        println(dateTime.toLocalDate + " " + dateTime.toLocalTime)
    }
}
