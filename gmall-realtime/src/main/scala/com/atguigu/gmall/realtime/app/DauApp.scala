package com.atguigu.gmall.realtime.app

import java.time.LocalDate

import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{EsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
 * Author lzc
 * Date 2020/8/13 8:18 下午
 */
object DauApp {
    
    
    def main(args: Array[String]): Unit = {
        // 1. 创建一个StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 获取启动日志流
        val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, "gmall_startup_topic")
        // 2.1 把json 字符串数据封装到一个样例类对象中
        val startupLogStream = parseToStartupLog(sourceStream)
        
        // 3. 按照 mid 对数据进行去重
        val filteredStartupLogStream = distinct(startupLogStream)
        filteredStartupLogStream.cache()
        // 4. 把数据写入到 es 中
        saveToES(filteredStartupLogStream)
        filteredStartupLogStream.print()
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    /**
     * 把流中的字符串解析为样例类 StartupLog 类型
     *
     * @param sourceStream
     * @return
     */
    private def parseToStartupLog(sourceStream: DStream[String]): DStream[StartupLog] = {
        sourceStream.map(jsonString => {
            val j: JValue = JsonMethods.parse(jsonString)
            val jCommon = j \ "common"
            val jTs = j \ "ts"
            implicit val d = org.json4s.DefaultFormats
            jCommon.merge(JObject("ts" -> jTs)).extract[StartupLog]
        })
    }
    
    /**
     * 按照 mid, 对流的数据进行去重
     * 待优化
     *
     * @param startupLogStream
     * @return
     */
    def distinct_1(startupLogStream: DStream[StartupLog]) = {
        val preKey = "mids:"
        startupLogStream.filter(startupLog => {
            val client: Jedis = RedisUtil.getClient
            val key = preKey + startupLog.logDate
            // 把 mid 写入到 Set 中, 如果返回值为 1 表示写入成功, 返回值为 0 表示不是首次写入, 数据重复
            val result = client.sadd(key, startupLog.mid)
            client.close()
            result == 1 // 首次写入的保留下来, 非首次写入的过滤掉
        })
    }
    
    /**
     * 按照 mid, 对流的数据进行去重
     * 优化后
     *
     * @param startupLogStream
     * @return
     */
    def distinct(startupLogStream: DStream[StartupLog]) = {
        val preKey = "mids:"
        startupLogStream.mapPartitions(startupLogIt => {
            val client: Jedis = RedisUtil.getClient
            val result = startupLogIt.filter(startupLog => {
                val key = preKey + startupLog.logDate
                val result = client.sadd(key, startupLog.mid)
                client.close()
                result == 1
            })
            client.close()
            result
        })
    }
    
    /**
     * 把数据写入到 es 中
     *
     * @param stream
     * @return
     */
    def saveToES(stream: DStream[StartupLog]) = {
        stream.foreachRDD(rdd => {
            rdd.foreachPartition(startupLogIt => {
                val today: String = LocalDate.now.toString
                // 使用gmall_dau_info_2020-08-20 作为 index
                EsUtil.insertBulk(s"gmall_dau_info_$today", startupLogIt)
            })
        })
    }
    
}
