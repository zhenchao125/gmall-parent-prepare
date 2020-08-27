package com.atguigu

/**
 * Author lzc
 * Date 2020/8/27 10:28 下午
 */
object ScalaTest {
    def main(args: Array[String]): Unit = {
        val list = A(10)::Nil
        list match {
            case head:: tail =>
                println(head)
        }
    }
}

case class A(var a: Int)
