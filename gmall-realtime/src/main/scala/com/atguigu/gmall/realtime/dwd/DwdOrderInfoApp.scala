package com.atguigu.gmall.realtime.dwd

import java.time.LocalDate

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.bean.{OrderInfo, UserStatus}
import com.atguigu.gmall.realtime.util.{EsUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/27 1:33 下午
 */
object DwdOrderInfoApp extends BaseApp {
    implicit val f = org.json4s.DefaultFormats
    
    override var appName: String = "DwdOrderInfoApp"
    override var groupId: String = "DwdOrderInfoApp"
    override var topic: String = "ods_order_info"
    
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        // 1. 把 json 格式数据封装到 OrderInfo 中
        val orderInfoStream = sourceStream.map(record => {
            JsonMethods.parse(record.value()).extract[OrderInfo]
        })
        
        // 2. 标记首单
        val firstOrderInfoStream = orderInfoStream
            .mapPartitions(orderInfoIt => {
                val orderInfoList = orderInfoIt.toList
                // 0. 获取所有的用户 id
                val userIds = orderInfoList.map(_.user_id).mkString("','")
                // 1. 连接 phoenix, 读每个 user 的的状态    "1"-> true, "2"->false, ...
                val userIdToConsumed = PhoenixUtil
                    .query(s"select user_id, is_consumed from user_status where user_id in ('${userIds}')", Nil)
                    .map(map => {
                        map("user_id").toString -> map("is_consumed").asInstanceOf[Boolean]
                    })
                    .toMap
                // 2. 如果订单的 id 包含在userIdToConsumed中, 则表示非首单, 否则表示首单
                orderInfoList.map(orderInfo => {
                    orderInfo.is_first_order = !userIdToConsumed.contains(orderInfo.user_id.toString)
                    orderInfo
                }).toIterator
            })
        // 3. 同一批次同一用户多首单问题解决
        val resultStream = firstOrderInfoStream
            .map(info => (info.user_id, info))
            .groupByKey()
            .flatMap {
                case (user_id, infoIt) =>
                    val orderInfoList: List[OrderInfo] = infoIt.toList
                    /*if (orderInfoList.size > 1) {
                        val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortBy(_.create_time)
                        sortedOrderInfoList match {
                            case head :: tail if head.is_first_order => // 如果最早的单是首单, 则其他单改为非首单
                                tail.foreach(_.is_first_order = false)
                            case _ =>
                        }
                        sortedOrderInfoList // 返回改完后的集合
                    } else {
                        orderInfoList
                    }*/
                    
                    val sortedOrderInfoList: List[OrderInfo] = orderInfoList.sortBy(_.create_time)
                    sortedOrderInfoList match {
                        case one :: two :: tail if one.is_first_order => // 至少两单, 且最早的单是首单, 则把把其他单设置为非首单
                            (two :: tail).foreach(_.is_first_order = false)
                            println("a: " + one :: two :: tail)
                        case a => println("b: " + a)
                    }
                    sortedOrderInfoList // 返回修改后的
            }
        
        // 4. 首单数据写入到 es, 并在 hbase(通过 Phoenix) 中维护用户的状态
        resultStream.foreachRDD(rdd => {
            import org.apache.phoenix.spark._
            rdd.cache()
            // 用户状态写入到 Phoenix
            rdd.filter(_.is_first_order)
                .map(info => UserStatus(info.user_id.toString, isConsumed = true))
                .saveToPhoenix("USER_STATUS", Seq("USER_ID", "IS_CONSUMED"), zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
            // 首单写入写入到 es
            rdd.foreachPartition(orderInfoIt => {
                EsUtil.insertBulk(s"gmall_order_info_${LocalDate.now()}", orderInfoIt.map(info => (info.id.toString, info)))
            })
            
            OffsetManager.saveOffsets(offsetRanges, groupId, topic)
        })
        
    }
}
