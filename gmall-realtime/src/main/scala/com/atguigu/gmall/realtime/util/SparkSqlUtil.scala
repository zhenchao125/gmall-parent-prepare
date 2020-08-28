package com.atguigu.gmall.realtime.util

import org.apache.spark.sql.{Encoder, SparkSession}

/**
 * Author lzc
 * Date 2020/8/28 9:44 上午
 */
object SparkSqlUtil {
    val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
    
    def getRDD[C: Encoder](spark: SparkSession, sql: String) = {
        spark.read
            .format("jdbc")
            .option("url", url)
            .option("query", sql)
            .load()
            .as[C]
            .rdd
    }
}
