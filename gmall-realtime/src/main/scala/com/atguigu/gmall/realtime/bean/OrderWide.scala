package com.atguigu.gmall.realtime.bean

/**
 * Author lzc
 * Date 2020/8/29 10:17 上午
 *
 * 1. 粒度和 order_detail 形同
 */
case class OrderWide( // 来源 OrderInfo
                      var order_id: Long = 0L,
                      var province_id: Long = 0L,
                      var order_status: String = null,
                      var user_id: Long = 0L,

                      var final_total_amount: Double = 0D, // 实际支付总金额 = 原始总金额-优惠+运费
                      var benefit_reduce_amount: Double = 0D, // 优惠金额
                      var original_total_amount: Double = 0D, // 原始总金额 = ∑sku_price*sku_num

                      var feight_fee: Double = 0D, // 运费
                      var expire_time: String = null,
                      var create_time: String = null,
                      var operate_time: String = null,
                      var create_date: String = null,
                      var create_hour: String = null,
                      var is_first_order: Boolean = false,

                      var province_name: String = null,
                      var province_area_code: String = null,
                      var province_iso_code: String = null,

                      var user_age_group: String = null,
                      var user_gender: String = null,

                      //开源: OderDetail
                      var order_detail_id: Long = 0L,
                      var sku_id: Long = 0L,
                      var sku_price: Double = 0L, // 在 OrderDetail 中叫 order_price
                      var sku_num: Long = 0L,
                      var sku_name: String = null,

                      var spu_id: Long = 0L,
                      var tm_id: Long = 0L,
                      var category3_id: Long = 0L,
                      var spu_name: String = null,
                      var tm_name: String = null,
                      var category3_name: String = null,

                      // 需要计算的分摊金额
                      var final_detail_amount: Double = 0D) {
    def this(orderInfo: OrderInfo, orderDetail: OrderDetail) {
        this
        mergeOrderInfo(orderInfo)
        mergeOrderDetail(orderDetail)
    }
    
    def mergeOrderInfo(orderInfo: OrderInfo) = {
        if (orderInfo != null) {
            this.order_id = orderInfo.id
            this.province_id = orderInfo.province_id
            this.order_status = orderInfo.order_status
            this.user_id = orderInfo.user_id
            this.final_total_amount = orderInfo.final_total_amount
            this.benefit_reduce_amount = orderInfo.benefit_reduce_amount
            this.original_total_amount = orderInfo.original_total_amount
            this.feight_fee = orderInfo.feight_fee
            this.expire_time = orderInfo.expire_time
            this.create_time = orderInfo.create_time
            this.create_date = orderInfo.create_date
            this.create_hour = orderInfo.create_hour
            this.is_first_order = orderInfo.is_first_order
            this.province_name = orderInfo.province_name
            this.province_area_code = orderInfo.province_area_code
            this.province_iso_code = orderInfo.province_iso_code
            this.user_age_group = orderInfo.user_age_group
            this.user_gender = orderInfo.user_gender
        }
        this
    }
    
    
    def mergeOrderDetail(orderDetail: OrderDetail) = {
        if (orderDetail != null) {
            this.order_detail_id = orderDetail.id
            this.sku_id = orderDetail.sku_id
            this.sku_num = orderDetail.sku_num
            this.sku_name = orderDetail.sku_name
            this.sku_price = orderDetail.order_price
            
            this.spu_id = orderDetail.spu_id
            this.tm_id = orderDetail.tm_id
            this.category3_id = orderDetail.category3_id
            this.spu_name = orderDetail.spu_name
            this.tm_name = orderDetail.tm_name
            this.category3_name = orderDetail.category3_name
        }
        this
    }
    
    override def toString: String = s"原始总金额: ${original_total_amount}, " +
        s"支付总金额: ${final_total_amount}, " +
        s"该详情总金额: ${sku_num * sku_price}, " +
        s"分摊金额: ${final_detail_amount}, " +
        s"订单 id: ${order_id}"
}
