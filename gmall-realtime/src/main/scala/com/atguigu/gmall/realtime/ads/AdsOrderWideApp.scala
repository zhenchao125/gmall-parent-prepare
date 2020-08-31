package com.atguigu.gmall.realtime.ads

import com.atguigu.gmall.realtime.BaseAppV4
import com.atguigu.gmall.realtime.bean.OrderWide
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

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
        
        trademarkToAmountStream.foreachRDD(rdd => {
        
        })
    }
}
