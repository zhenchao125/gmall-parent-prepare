package com.atguigu.gmall.realtime.dwd

import java.util.Properties

import com.atguigu.gmall.realtime.BaseAppV2
import com.atguigu.gmall.realtime.bean._
import com.atguigu.gmall.realtime.util.OffsetManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.Formats
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/28 5:30 下午
 *
 * 把所有需要的维度表导入到 hbse 中
 * 1. 优化 BaseApp
 * 2. 优化 OffsetManage 的设计
 */
object DwdDimApp extends BaseAppV2 {
    implicit val f = org.json4s.DefaultFormats
    
    override var appName: String = "DwdDimApp"
    override var groupId: String = "DwdDimApp"
    override var totalCores: Int = 2
    override var topics: Set[String] = Set(
        "ods_user_info",
        "ods_sku_info",
        "ods_spu_info",
        "ods_base_category3",
        "ods_base_province",
        "ods_base_trademark")
    
    
    def save[A <: Product](rdd: RDD[(String, String)], topic: String, tableName: String, cols: Seq[String])(implicit formats: Formats, mf: scala.reflect.Manifest[A]) {
        import org.apache.phoenix.spark._
        rdd.filter(_._1 == topic)
            .map(_._2)
            .map(json => {
                JsonMethods.parse(json).extract[A](f, mf)
            })
            .saveToPhoenix(
                tableName,
                cols,
                zkUrl = Option("hadoop102,hadoop1hadoop104:2181"))
    }
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     sourceStream: DStream[ConsumerRecord[String, String]]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        import spark.implicits._
        
        
        sourceStream
            .map(record => (record.topic(), record.value())) // 因为数据来源于多个 topic, 所以需要知道每条数据所属的 topic
            .foreachRDD(rdd => {
                rdd.cache()
                topics.foreach {
                    case "ods_user_info" =>
                        save[UserInfo](rdd,
                            "ods_user_info",
                            "gmall_user_info",
                            Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"))
                    
                    case "ods_sku_info" =>
                        save[SkuInfo](rdd,
                            "ods_sku_info",
                            "gmall_sku_info",
                            Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"))
                        // 需要和 gmall_spu_info gmall_base_category3  gmall_base_trademark 连接, 然后得到所有字段
                        // 使用 spark-sql 完成
                        import org.apache.phoenix.spark._
                        val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
                        spark.read.jdbc(url, "gmall_sku_info", new Properties()).createOrReplaceTempView("sku")
                        spark.read.jdbc(url, "gmall_spu_info", new Properties()).createOrReplaceTempView("spu")
                        spark.read.jdbc(url, "gmall_base_category3", new Properties()).createOrReplaceTempView("category3")
                        spark.read.jdbc(url, "gmall_base_trademark", new Properties()).createOrReplaceTempView("tm")
                        spark.sql(
                            """
                              |select
                              |    sku.id as id,
                              |    sku.spu_id spu_id,
                              |    sku.price price,
                              |    sku.sku_name sku_name,
                              |    sku.tm_id  tm_id,
                              |    sku.category3_id  category3_id,
                              |    sku.create_time  create_time,
                              |    category3.name  category3_name,
                              |    spu.spu_name  spu_name,
                              |    tm.tm_name  tm_name
                              |from sku
                              |join spu on sku.spu_id=spu.id
                              |join category3 on sku.category3_id=category3.id
                              |join tm on sku.tm_id=tm.id
                              |""".stripMargin)
                            .as[SkuInfo]
                            .rdd
                            .saveToPhoenix(
                                "gmall_sku_info",
                                Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
                                zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
                    
                    case "ods_spu_info" =>
                        save[SpuInfo](rdd,
                            "ods_spu_info",
                            "gmall_spu_info",
                            Seq("ID", "SPU_NAME"))
                    
                    case "ods_base_category3" =>
                        save[BaseCategory3](rdd,
                            "ods_base_category3",
                            "gmall_base_category3",
                            Seq("ID", "NAME", "CATEGORY2_ID"))
                    
                    case "ods_base_province" =>
                        save[ProvinceInfo](rdd,
                            "ods_base_province",
                            "gmall_province_info",
                            Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"))
                    
                    case "ods_base_trademark" =>
                        save[BaseTrademark](rdd,
                            "ods_base_trademark",
                            "gmall_base_trademark",
                            Seq("ID", "TM_NAME"))
                    
                    
                    case topic => throw new UnsupportedOperationException(s"不支持消费此 ${topic}")
                    
                }
                
                OffsetManager.saveOffsets(offsetRanges, groupId, topics)
            })
        
    }
    
    
}
