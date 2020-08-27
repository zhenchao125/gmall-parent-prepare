package com.atguigu

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, streaming}

/**
 * Author lzc
 * Date 2020/8/27 7:19 下午
 */
object SteamTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(3))
        val s1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        s1.foreachRDD(rdd =>{
            println(rdd.collect().mkString(", "))
            println(rdd.collect().mkString(", "))
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
