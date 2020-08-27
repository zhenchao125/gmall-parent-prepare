package com.atguigu.gmall.realtime.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author lzc
 * Date 2020/8/27 7:24 下午
 */
object StreamingTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(3))
        val s1: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, "a")
        s1.foreachRDD(rdd => {
            println(rdd.collect().mkString(", "))
            println(rdd.collect().mkString(", "))
        })
        
        ssc.start()
        ssc.awaitTermination()
    }
}
