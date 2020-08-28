package com.atguigu.gmall.realtime.bean

/**
 * Author lzc
 * Date 2020/8/28 9:35 下午
 */
case class SkuInfo(id: String,
                   spu_id: String,
                   price: String,
                   sku_name: String,
                   tm_id: String,
                   category3_id: String,
                   create_time: String,

                   var category3_name: String = null,
                   var spu_name: String = null,
                   var tm_name: String = null)