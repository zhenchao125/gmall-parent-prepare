package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.TopicPartition

/**
 * Author lzc
 * Date 2020/8/16 7:22 下午
 */
object OffsetManagerV2 {
    def readOffset(groupId:String, topic: String) = {
        val url = "jdbc:mysql://hadoop102:3306/gmall_result?characterEncoding=utf-8&useSSL=false&user=root&password=aaaaaa"
        val sql =
            """
              |select
              | *
              |from ads_offset
              |where topic=? and group_id=?
              |""".stripMargin
        JDBCUtil
            .query(url, sql, List(topic, groupId))
            .map(row => {
                val partitionId = row("partition_id").toString.toInt
                val partitionOffset = row("partition_offset").toString.toLong
                (new TopicPartition(topic, partitionId), partitionOffset)
            })
            .toMap
    }
    
    // 写偏移量这块将来要和写数据在一个事务中, 会用专门支持事务的工具完成, 此处略
}
