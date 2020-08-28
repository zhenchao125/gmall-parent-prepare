package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/16 7:22 下午
 */
object OffsetManager {
    /**
     * 从 redis 读取 offset
     *
     * @param groupId
     * @param topic
     * @return
     */
    def readOffsets(groupId: String, topic: String): Map[TopicPartition, Long] = {
        val client: Jedis = RedisUtil.getClient
        val partitionToOffsetMap: util.Map[String, String] = client.hgetAll(s"offset:${groupId}:${topic}")
        client.close()
        import scala.collection.JavaConverters._
        partitionToOffsetMap
            .asScala
            .map {
                case (partition, offset) =>
                    new TopicPartition(topic, partition.toInt) -> offset.toLong
            }
            .toMap
    }
    
    /**
     * 把 offset 存储到 redis 中
     *
     * @param offsetRanges
     */
    def saveOffsets(offsetRanges: ListBuffer[OffsetRange], groupId: String, topic: String): Unit = {
        if (offsetRanges.isEmpty) return
        import scala.collection.JavaConverters._
        // 获取分区和该分区的偏移量
        val fieldToValue = offsetRanges
            .map(offsetRange => {
                offsetRange.partition.toString -> offsetRange.untilOffset.toString
            })
            .toMap
            .asJava
        val client: Jedis = RedisUtil.getClient
        println("topic->offset: " + fieldToValue)
        client.hmset(s"offset:${groupId}:${topic}", fieldToValue)
        client.close()
    }
    
    //------------
    
    def readOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
        import scala.collection.JavaConverters._
        val client: Jedis = RedisUtil.getClient
        val partitionToOffsetMap = topics.map(topic => {
            client
                .hgetAll(s"offset:${groupId}:${topic}")
                .asScala
                .map {
                    case (partition, offset) =>
                        new TopicPartition(topic, partition.toInt) -> offset.toLong
                }
                .toMap
        }).reduce((map1, map2) => {
            map1 ++ map2
        })
        client.close()
        partitionToOffsetMap
    }
    
    
    def saveOffsets(offsetRanges: ListBuffer[OffsetRange], groupId: String, topics: Set[String]): Unit = {
        if (offsetRanges.isEmpty) return
        val client: Jedis = RedisUtil.getClient
        offsetRanges.foreach(offsetRange => {
            println(
                s"""
                   |===========
                   |topic: partition ->  offset: ${offsetRange.topic}: ${offsetRange.partition} -> ${offsetRange.untilOffset}
                   | ==========""".stripMargin)
            client.hset(s"offset:${groupId}:${offsetRange.topic}", offsetRange.partition.toString, offsetRange.untilOffset.toString)
        })
        client.close()
    }
    
}
