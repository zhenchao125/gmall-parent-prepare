package com.atguigu.gmall.realtime.dwd

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.util.OffsetManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/28 2:35 上午
 */
object DwdDimUserApp extends BaseApp {
    override var appName: String = "DwdDimUserApp"
    override var groupId: String = "DwdDimUserApp"
    override var topic: String = "ods_user_info"
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        sourceStream
            .map(record => {
                implicit val f = org.json4s.DefaultFormats
                JsonMethods.parse(record.value()).extract[UserInfo]
            })
            .foreachRDD(rdd => {
                import org.apache.phoenix.spark._
                rdd.saveToPhoenix("gmall_user_info",
                    Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"),
                    zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
                
                OffsetManager.saveOffsets(offsetRanges, groupId, topic)
            })
    }
}
