package com.atguigu.gmall.realtime

import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/26 7:58 下午
 */
abstract class BaseAppV3 {
    var appName: String
    var groupId: String
    var totalCores: Int
    var topics: Set[String]
    
    def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], topicToStreamMap: Map[String, DStream[ConsumerRecord[String, String]]])
    
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf().setMaster(s"local[$totalCores]").setAppName(appName)
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        
        val fromOffsets: Map[TopicPartition, Long] = OffsetManager.readOffsets(groupId, topics)
        println(fromOffsets)
        val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]
        
        val topicToStreamMap = topics
            .map(topic => {
                val ownOffsets = fromOffsets.filter(_._1.topic() == topic)
                val stream = MyKafkaUtil
                    .getKafkaStream(ssc, groupId, topic, ownOffsets)
                    .transform(rdd => {
                        val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                        offsetRanges ++= newOffsetRanges
                        rdd
                    })
                (topic, stream)
            })
            .toMap // 转成 map 的目的是方便操作, 取出需要的流
        
        run(ssc, offsetRanges, topicToStreamMap)
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list hadoop102:9092 --topic dwd_order_info --time -1
 */
