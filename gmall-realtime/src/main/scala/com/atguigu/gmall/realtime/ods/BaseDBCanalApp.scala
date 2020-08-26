package com.atguigu.gmall.realtime.ods

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/26 7:57 下午
 */
object BaseDBCanalApp extends BaseApp {
    override var appName: String = "BaseDBCanalApp"
    override var groupId: String = "bigdata1"
    override var topic: String = "gmall_db"
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        sourceStream
            .flatMap(record => {
                val j: JValue = JsonMethods.parse(record.value())
                val data: JValue = j \ "data"
                implicit val f = org.json4s.DefaultFormats
                val tableName = JsonMethods.render(j \ "table").extract[String]
                println(tableName)
                data.children.map(child => {
                    (tableName, JsonMethods.compact(JsonMethods.render(child)))
                })
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
                    it.foreach {
                        case (tableName, content) =>
                            val topic = s"ods_${tableName}"
                            producer.send(new ProducerRecord[String, String](topic, content))
                    }
                    producer.close()
                })
                
                OffsetManager.saveOffsets(offsetRanges, groupId, topic)
            })
    }
}
