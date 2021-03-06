package com.atguigu.gmall.realtime

import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/26 7:58 下午
 */
abstract class BaseApp {
    var appName: String
    var groupId: String
    var topic: String
    
    def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ConsumerRecord[String, String]])
    
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        
        
        val fromOffsets: Map[TopicPartition, Long] = OffsetManager.readOffsets(groupId, topic)
        val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]
        
        val sourceStream: DStream[ConsumerRecord[String, String]] = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topic, fromOffsets)
            .transform(rdd => {
                offsetRanges.clear
                val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                offsetRanges ++= newOffsetRanges
                rdd
            })
        
        run(ssc, offsetRanges, sourceStream)
        
        ssc.start()
        ssc.awaitTermination()
    }
}
