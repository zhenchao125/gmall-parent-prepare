package com.atguigu.gmall.realtime.util

import java.io.InputStream
import java.util.Properties

/**
 * Author lzc
 * Date 2020/8/13 6:58 下午
 */
object ConfigUtil {
    private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
    private val properties = new Properties()
    properties.load(is)
    
    def getProperty(propertyName: String): String = properties.getProperty(propertyName)
}
