package com.atguigu.gmall.realtime.dws

import java.lang

import com.atguigu.gmall.realtime.BaseAppV3
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/29 10:52 上午
 */
object DwsOrderWideApp extends BaseAppV3 {
    override var appName: String = "DwsOrderWideApp"
    override var groupId: String = "DwsOrderWideApp1"
    override var totalCores: Int = 2
    override var topics: Set[String] = Set("dwd_order_info", "dwd_order_detail")
    
    override def run(ssc: StreamingContext,
                     offsetRanges: ListBuffer[OffsetRange],
                     topicToStreamMap: Map[String, DStream[ConsumerRecord[String, String]]]): Unit = {
        
        // 1. 得到两个事实表的流, 为了 join 不丢数据, 并添加窗口
        val orderInfoStream = topicToStreamMap("dwd_order_info")
            .map(record => {
                implicit val f = org.json4s.DefaultFormats
                val orderInfo = JsonMethods.parse(record.value()).extract[OrderInfo]
                (orderInfo.id, orderInfo)
            })
            .window(Seconds(24), Seconds(3))
        val orderDetailStream = topicToStreamMap("dwd_order_detail")
            .map(record => {
                implicit val f = org.json4s.DefaultFormats
                val orderDetail = JsonMethods.parse(record.value()).extract[OrderDetail]
                (orderDetail.order_id, orderDetail)
            })
            .window(Seconds(24), Seconds(3))
        
        // 2. 对2 个流进行 join  此处使用内连接
        val orderWideStream: DStream[OrderWide] = orderInfoStream.join(orderDetailStream).map {
            case (_, (orderInfo, orderDetail)) => new OrderWide(orderInfo, orderDetail)
        }
        
        // 3. 对重复 join 的数据进行去重
        val orderWideDistinctStream = orderWideStream.mapPartitions(orderWideIt => {
            val client: Jedis = RedisUtil.getClient
            val result = orderWideIt.filter(orderWide => {
                val oneOrZero = client.sadd(s"order_join:${orderWide.order_id}", orderWide.order_detail_id.toString)
                client.expire(s"order_join:${orderWide.order_id}", 60) // 给每个 key 单独设置过期时间
                oneOrZero == 1
            })
            client.close()
            result
        })
        // 4. 计算分摊金额
        val result = orderWideDistinctStream.mapPartitions(orderWideIt => {
            val client: Jedis = RedisUtil.getClient
            
            val r = orderWideIt.map(orderWide => {
                val preTotalKey = s"pre_total:${orderWide.order_id}"
                val preSharesKey = s"pre_shares:${orderWide.order_id}"
                // 1. 获取前面详情的原始总金额(不包括当前详情的)
                val preTotalTemp = client.get(preTotalKey)
                val preTotal = if (preTotalTemp == null) 0D else preTotalTemp.toDouble
                // 2. 获取前面详情的分摊总金额(不包括当前详情的)
                val preSharesTemp = client.get(preSharesKey)
                val preShares = if (preSharesTemp == null) 0D else preSharesTemp.toDouble
                // 3. 判断是否最后一个详情
                val current: Double = orderWide.sku_price * orderWide.sku_num
                if (current == orderWide.original_total_amount - preTotal) { // 个数*成单价 == 该单原始总金额 - ∑前面详情的个数*单价
                    // 3.1 是最后一详情    分摊金额 = 实际付款总金额 - ∑前面分摊总金额
                    orderWide.final_detail_amount = orderWide.final_total_amount - preShares
                    // 删除 redis 的对应的 key
                    client.del(preTotalKey)
                    client.del(preSharesKey)
                    println("最后一详情")
                } else {
                    // 3.2 不是最后一详情   分摊金额 = 个数*单价*实际付款金额/该单原始总金额
                    val share: Double = orderWide.sku_num * orderWide.sku_price * orderWide.final_total_amount / orderWide.original_total_amount
                    // 3.33333333 * 100 => 333.33333 => 333 => 3.33
                    orderWide.final_detail_amount = Math.round(share * 100) / 100
                    // 保存  ∑前面分摊总金额  ∑前面详情的个数*单价
                    val d1: lang.Double = client.incrByFloat(preTotalKey, current)
                    val d2: lang.Double = client.incrByFloat(preSharesKey, Math.round(share * 100) / 100)
                    println(s"不是最后一详情: ${d1}, ${d2}")
                }
                orderWide
            })
            client.close()
            r
        })
        
        
        val spark: SparkSession = SparkSession
            .builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        
        // 5. 写入到 ClickHouse
        result.foreachRDD(rdd => {
            
            // 写入到 clickhouse
            /*rdd.cache()
            println("时间戳.....开始")
            val df: Dataset[OrderWide] = rdd.toDS()
            df.show(1000)
            df
                .write
                .option("batchsize", "100")
                .option("isolationLevel", "NONE") // 设置没有事务
                .option("numPartitions", "2") // 设置并发
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .mode("append")
                .jdbc("jdbc:clickhouse://hadoop102:8123/gmall", "order_wide", new Properties())
            println("时间戳.....结束")*/
            
            // 写入到 kafka 的 dws 层
            println("时间戳.....开始")
            rdd.foreachPartition(orderWideIt => {
                val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
                orderWideIt.foreach(orderWide => {
                    implicit val f = org.json4s.DefaultFormats
                    producer.send(new ProducerRecord[String, String]("dwd_order_wide", Serialization.write(orderWide)))
                })
                producer.close()
            })
            println("时间戳.....结束")
            OffsetManager.saveOffsets(offsetRanges, groupId, topics)
        })
        
    }
}

/*
1. 如何判断是否重复 join
    
        类似于前面的判断日活
        
        重复 join 的时候"order_detail_id" 肯定是相同的
        所以, 把 "order_detail_id" 存入 set 中
        1. 第一次存入, 返回 1  (不重复)
        2. 第二次存入, 返回 0 (重复)
        3. 把重复的过滤掉(返回值为0 的去掉)
        
        key                                     value(set)
        "order_join:${order_id}"                 "1_2", "1_3",...   order_detail_id, ...
        
        说明: 每个order 一个key, 方便对 key 进行过期自动删除. 否则会长期占用内存
        
2. 分摊逻辑

    1.	按比例求分摊: 分摊金额/实际付款金额 = 个数*单价/该单原始总金额
    2.	所以: 分摊金额 = 个数*单价*实际付款金额/该单原始总金额
    3.	由于有除法的存在, 结果我们需要做四舍五入, 会导致精度的丢失
    4.	一个订单对应多个详情, 每个详情均做四舍五入, 他们的和可能与该订单的实际支付总金额不等
                订单              详情 1   详情 2    详情 3
           原始: 120            	    40		40         40
           分摊: 100				    33.33    33.33	   33.33
    5.	需要消除这个bug
        如果有 3 个详情:
            前面的详情使用乘除:    分摊金额 = 个数*单价*实际付款金额/该单原始总金额
            最后一个详情使用减法:  分摊金额 = 实际付款总金额 - ∑前面分摊总金额
            
    6.	难点: 如何知道最后一个详情
             当前详情
                个数*成单价 == 该单原始总金额 - ∑前面详情的个数*单价
                
    7.	需要记录的状态:
            ∑前面分摊总金额
            ∑前面详情的个数*单价

 */
