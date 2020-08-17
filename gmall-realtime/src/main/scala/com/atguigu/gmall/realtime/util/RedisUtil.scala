package com.atguigu.gmall.realtime.util

import redis.clients.jedis.Jedis

/**
 * Author lzc
 * Date 2020/8/13 7:18 下午
 */
object RedisUtil {
    val host = ConfigUtil.getProperty("redis.host")
    val port = ConfigUtil.getProperty("redis.port").toInt
    
    def getClient = {
        new Jedis(host, port)
    }
}
