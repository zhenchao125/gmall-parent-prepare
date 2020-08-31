package com.atguigu.gmall.realtime.ads

import java.time.LocalDateTime

import com.atguigu.gmall.realtime.BaseAppV4
import com.atguigu.gmall.realtime.bean.OrderWide
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/31 2:25 下午
 */
object AdsOrderWideApp extends BaseAppV4 {
    override var appName: String = "AdsOrderWideApp"
    override var groupId: String = "AdsOrderWideApp"
    override var topic: String = "dwd_order_wide"
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        
        // 1. 没 3 秒统计一次 每个品的交易额
        val trademarkToAmountStream: DStream[((Long, String), Double)] = sourceStream
            .map(record => {
                implicit val f = org.json4s.DefaultFormats
                val orderWide: OrderWide = JsonMethods.parse(record.value()).extract[OrderWide]
                ((orderWide.tm_id, orderWide.tm_name), orderWide.final_detail_amount)
            })
            .reduceByKey(_ + _)
        
        // 2. 数据写入到 mysql
        // scalalike jdbc 加载配置
        DBs.setup()
        trademarkToAmountStream.foreachRDD(rdd => {
            val tmAndAmount: Array[((Long, String), Double)] = rdd.collect()
            tmAndAmount.foreach(println)
            println(offsetRanges)
            if (tmAndAmount.nonEmpty) {
                // 所有的操作都会处于同一个事务中
                DB.localTx(implicit session => {
                    // 插入数据
                    val dt = LocalDateTime.now()
                    val stat_time = s"${dt.toLocalDate}_${dt.toLocalTime}"
                    // 使用批次提交: 批次中的参数 每个 Seq 存储一行 sql 的参数
                    val dataBatchParamList: List[Seq[Any]] = tmAndAmount.map {
                        case ((tm_id, tm_name), amount) =>
                            Seq(stat_time, tm_id, tm_name, amount)
                    }.toList
                    val insertDataSql =
                        """
                          |insert into tm_amount values(?, ?, ?, ?)
                          |""".stripMargin
                    SQL(insertDataSql).batch(dataBatchParamList: _*).apply()
                    
                    // 插入或更新 offset
                    val offsetBatchParamList = offsetRanges.map(offsetRange => {
                        Seq(groupId, topic, offsetRange.partition, offsetRange.untilOffset)
                    })
                    val insertOffsetSql =
                        """
                          |replace into ads_offset values(?, ?, ?, ?)
                          |""".stripMargin
                    SQL(insertOffsetSql).batch(offsetBatchParamList: _*).apply()
                })
            }
        })
    }
}
