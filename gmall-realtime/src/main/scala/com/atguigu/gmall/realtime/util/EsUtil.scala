package com.atguigu.gmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
 * Author atguigu
 * Date 2020/7/20 13:58
 */
object EsUtil {
    val esUrl = "http://hadoop102:9200"
    // 创建es客户端
    val factory = new JestClientFactory
    val conf = new HttpClientConfig.Builder(esUrl)
        .connTimeout(1000 * 10)
        .readTimeout(1000 * 10)
        .maxTotalConnection(100)
        .multiThreaded(true)
        .build()
    factory.setHttpClientConfig(conf)
    
    
    def main(args: Array[String]): Unit = {
        insertSingle("user", User("a", 100))
        
        /* val it1 = List(("1", User("cc", 1)), ("2", User("dd", 2))).toIterator
         val it2 = List(User("cc", 1), User("dd", 2)).toIterator
         insertBulk("user", it1)*/
        
    }
    
    def insertBulk(index: String, source: Iterator[Object]) = {
        val client: JestClient = factory.getObject
        val bulkBuilder = new Bulk.Builder()
            .defaultIndex(index)
            .defaultType("_doc")
        
        source.foreach {
            case (id: String, source) =>
                val builder = new Index.Builder(source).id(id)
                bulkBuilder.addAction(builder.build())
            case source =>
                val builder = new Index.Builder(source)
                bulkBuilder.addAction(builder.build())
        }
        client.execute(bulkBuilder.build())
        client.shutdownClient()
    }
    
    // 插入单条数据
    def insertSingle(index: String, source: Object, id: String = null): Unit = {
        val client: JestClient = factory.getObject
        val action: Index = new Index.Builder(source)
            .index(index)
            .`type`("_doc")
            .id(id)
            .build()
        client.execute(action)
        
        client.shutdownClient()
    }
    
    
}

case class User(name: String, age: Long)
