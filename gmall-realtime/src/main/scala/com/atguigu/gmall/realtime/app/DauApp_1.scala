package com.atguigu.gmall.realtime.app

import java.time.LocalDate

import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{EsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/13 8:18 下午
 */
object DauApp_1 {
    
    val groupId = "atguigu"
    val topic = "gmall_startup_topic"
    
    def main(args: Array[String]): Unit = {
        // 1. 创建一个StreamingContext
        val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        // 启动实时系统时, 读取上次停止的位置
        val fromOffsets = OffsetManager.readOffsets(groupId, topic)
        println(fromOffsets)
        // 需要保存的 offset. 保存的时候需要每次从这里读取数据, 需要使用可变集合,不能使用不可变集合
        val offsetRanges = ListBuffer.empty[OffsetRange]
        
        val sourceStream = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topic, fromOffsets)
            .transform(rdd => { // 此 transform 只获取这次的 offset 的位置.注意: 只能在此处获取
                // 先清空存储 offsetRanges 的集合
                offsetRanges.clear
                // 强转 rdd为OffsetRange, 然后获取 offsetRanges
                val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                // 再把新读到的 offset 储存进去
                offsetRanges ++= newOffsetRanges
                rdd
            })
        
        val startupLogStream = parseToStartupLog(sourceStream.map(_.value()))
        
        val filteredStartupLogStream = distinct(startupLogStream)
        filteredStartupLogStream.cache()
        saveToES(filteredStartupLogStream, offsetRanges)
        
        
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
     * 并把偏移量写入到 redis 中
     *
     * @param stream
     * @param offsetRanges
     */
    def saveToES(stream: DStream[StartupLog], offsetRanges: ListBuffer[OffsetRange]) = {
        stream.foreachRDD(rdd => {
            rdd.foreachPartition(startupLogIt => {
                val today: String = LocalDate.now.toString
                // 使用gmall_dau_info_2020-08-20 作为 index, 虽然前面已经去重了, 为了体现 es 的幂等性,我们使用 mid 作为 document 的 id 来避免重复
                EsUtil.insertBulk(s"gmall_dau_info_$today", startupLogIt.map(log => log.mid -> log))
            })
            // 数据保存到 es 之后, 保存 offsets.
            OffsetManager.saveOffsets(offsetRanges, groupId, topic)
        })
    }
}
