package com.atguigu

import java.util.Properties

import com.atguigu.gmall.realtime.bean.UserInfo
import org.apache.spark.sql.SparkSession

/**
 * Author lzc
 * Date 2020/8/28 9:20 上午
 */
object SparkSqlDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        spark
            .read
            .jdbc("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181",
                "GMALL_USER_INFO",
                new Properties())
            .as[UserInfo]
            .show()
        
        
        spark.close()
    }
}
