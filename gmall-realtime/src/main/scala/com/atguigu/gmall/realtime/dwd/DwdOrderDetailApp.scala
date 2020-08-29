package com.atguigu.gmall.realtime.dwd

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager, SparkSqlUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JInt, JLong, JString}
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/29 7:23 上午
 */
object DwdOrderDetailApp extends BaseApp {
    override var appName: String = "DwdOrderDetailApp"
    override var groupId: String = "DwdOrderDetailApp"
    override var topic: String = "ods_order_detail"
    
    object StringToLong extends CustomSerializer[Long](formats => ( {
        case JString(x) => x.toLong
        case JInt(x) => x.toLong
    }, {
        case x: Long => JLong(x)
        case x: Int => JInt(x)
    }))
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        // 1. 数据封装
        val orderDetailStream = sourceStream.map(record => {
            implicit val f = org.json4s.DefaultFormats + StringToLong
            System.out.println(record.value());
            JsonMethods.parse(record.value()).extract[OrderDetail]
        })
        // 2. 关联 sku_info 维度表
        val spark: SparkSession = SparkSession
            .builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        import spark.implicits._
        orderDetailStream.foreachRDD(rdd => {
            rdd.cache()
            // 2.1 获取素有需要的 SkuId
            val skuIds = rdd.map(_.sku_id).collect().distinct
            if (skuIds.length > 0) {
                // 2.2 skuId-> orderDetail
                val skuIdToOrderDetail = rdd.map(orderDetail => (orderDetail.sku_id.toString, orderDetail))
                
                // 2.2 加载 SkuInfo
                val skuIdToSkuInfo = SparkSqlUtil
                    .getRDD[SkuInfo](spark, s"select * from gmall_sku_info where id in('${skuIds.mkString("','")}')")
                    .map(skuInfo => (skuInfo.id, skuInfo))
                
                // 2.3 skuIdToOrderDetail join skuIdToSkuInfo
                skuIdToOrderDetail.join(skuIdToSkuInfo).map {
                    case (_, (orderDetail, skuInfo)) => orderDetail.mergeSkuInfo(skuInfo)
                }.foreachPartition(orderDetailIt => {
                    // 2.4 把 orderDetail 信息写入到 kafka
                    val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
                    orderDetailIt.foreach(orderDetail => {
                        implicit val f = org.json4s.DefaultFormats
                        producer.send(new ProducerRecord[String, String]("dwd_order_detail", Serialization.write(orderDetail)))
                    })
                    producer.close()
                })
            }
            OffsetManager.saveOffsets(offsetRanges, groupId, topic)
        })
    }
}
