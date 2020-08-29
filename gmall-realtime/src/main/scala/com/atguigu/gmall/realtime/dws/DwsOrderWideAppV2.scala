package com.atguigu.gmall.realtime.dws

import java.lang

import com.atguigu.gmall.realtime.BaseAppV3
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/29 10:52 上午
 */
object DwsOrderWideAppV2 extends BaseAppV3 {
    override var appName: String = "DwsOrderWideAppV2"
    override var groupId: String = "DwsOrderWideAppV2"
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
        val orderDetailStream = topicToStreamMap("dwd_order_detail")
            .map(record => {
                implicit val f = org.json4s.DefaultFormats
                val orderDetail = JsonMethods.parse(record.value()).extract[OrderDetail]
                (orderDetail.order_id, orderDetail)
            })
        
        // 2. 对2 个流进行 join  必须使用全连接
        val orderWideStream: DStream[OrderWide] = orderInfoStream
            .fullOuterJoin(orderDetailStream)
            .mapPartitions(it => {
                // 1. 建立到redis的连接
                val client: Jedis = RedisUtil.getClient
                // 2. 涉及到redis的操作
                val result = it.flatMap {
                    // order_info 和order_detail的数据同时到达
                    case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                        println(s"${orderId}  some some")
                        // 1. 把order_info信息写入到缓存
                        cacheOrderInfo(client, orderInfo)
                        // 2. 把order_info的信息和oder_detail的信息封装到一起
                        val orderWide: OrderWide = OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        // 3. 去order_detail的缓存中查找对应的信息.  注意: 需要删除order_detail中的信息
                        // 3.1 先获取根order_id相关的所有的key 3.2. 根据key回去对应的value(order_detail)
                        import scala.collection.JavaConverters._
                        val orderWides: mutable.Set[OrderWide] = client.keys(s"order_detail:${orderInfo.id}:*").asScala.map(key => {
                            val orderDetailJson: String = client.get(key)
                            implicit val f = org.json4s.DefaultFormats
                            val orderDetail: OrderDetail = JsonMethods.parse(orderDetailJson).extract[OrderDetail]
                            // order_detail的缓存中的数据join成功之后,需要删除, 否则会出现重复数据
                            client.del(key)
                            OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                        
                        orderWides += orderWide
                        orderWides
                    // order_info 和order_detail的数据没有同时到达
                    case (orderId, (Some(orderInfo), None)) =>
                        println(s"${orderId} some none")
                        // 1. 把order_info信息写入到缓存
                        cacheOrderInfo(client, orderInfo)
                        // 2. 去order_detail的缓存中查找对应的信息.  注意: 需要删除order_detail中的信息
                        import scala.collection.JavaConverters._
                        val orderWides: mutable.Set[OrderWide] = client.keys(s"order_detail:${orderInfo.id}:*").asScala.map(key => {
                            val orderDetailJson: String = client.get(key)
                            implicit val f = org.json4s.DefaultFormats
                            val orderDetail: OrderDetail = JsonMethods.parse(orderDetailJson).extract[OrderDetail]
                            // order_detail的缓存中的数据join成功之后,需要删除, 否则会出现重复数据
                            client.del(key)
                            OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                        orderWides
                    case (orderId, (None, Some(orderDetail))) =>
                        println(s"${orderId} none some")
                        // 1. 先去缓存查找对应的orderInfo
                        val orderInfoString: String = client.get(s"order_info:${orderDetail.order_id}")
                        // 2. 如果找到, 则组合成saleDetail. 如果没有找到, 应该把order_detail缓存
                        if (orderInfoString != null) {
                            implicit val f = org.json4s.DefaultFormats
                            val orderInfo = JsonMethods.parse(orderInfoString).extract[OrderInfo]
                            OrderWide().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                        } else {
                            // a: 先把order_details缓存
                            cacheOrderDetail(client, orderDetail)
                            // b: 返回空集合
                            Nil
                        }
                }
                client.close()
                result
            })
        // 3. 计算分摊金额
        val result = orderWideStream.mapPartitions(orderWideIt => {
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
        
        // 4. 写入到 ClickHouse
        result.foreachRDD(rdd => {
            rdd.cache()
            println("时间戳.....开始")
            rdd.collect().foreach(println)
            println("时间戳.....结束")
            
            //            OffsetManager.saveOffsets(offsetRanges, groupId, topics)
        })
        
        
    }
    
    
    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo): Unit = {
        implicit val f = org.json4s.DefaultFormats
        client.setex(s"order_info:${orderInfo.id}", 60 * 10, Serialization.write(orderInfo))
    }
    
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail): Unit = {
        implicit val f = org.json4s.DefaultFormats
        client.setex(s"order_detail:${orderDetail.order_id}:${orderDetail.id}", 60 * 10, Serialization.write(orderDetail))
    }
}
