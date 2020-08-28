package com.atguigu

import org.json4s.jackson.JsonMethods

/**
 * Author lzc
 * Date 2020/8/27 10:28 下午
 */
object ScalaTest {
    def main(args: Array[String]): Unit = {
        println(JsonMethods.parse("{}") \ "data" children)
    }
}

case class A(var a: Int)
