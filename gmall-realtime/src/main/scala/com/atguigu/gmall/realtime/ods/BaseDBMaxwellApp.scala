package com.atguigu.gmall.realtime.ods

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/26 11:57 下午
 */
object BaseDBMaxwellApp extends BaseApp {
    override var appName: String = "BaseDBMaxwellApp"
    override var groupId: String = "bigdata2"
    override var topic: String = "maxwell_gmall_db"
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        sourceStream
            .map(record => {
                val j: JValue = JsonMethods.parse(record.value())
                val data: JValue = j \ "data"
                implicit val f = org.json4s.DefaultFormats
                val tableName = JsonMethods.render(j \ "table").extract[String]
                (tableName, Serialization.write(data))
                
            })
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
                    it.foreach {
                        case (tableName, content) =>
                            val topic = s"ods_${tableName}"
                            println(topic)
                            producer.send(new ProducerRecord[String, String](topic, content))
                    }
                    producer.close()
                })
                
                OffsetManager.saveOffsets(offsetRanges, groupId, topic)
            })
    }
}
