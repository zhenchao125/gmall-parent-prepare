package com.atguigu

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.phoenix.jdbc.PhoenixConnection


/**
 * Author lzc
 * Date 2020/8/27 10:20 上午
 */
object PhoenixDemo {
    /*def main(args: Array[String]): Unit = {
        val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
        println(url)
        val conn = DriverManager.getConnection(url)
        conn.setAutoCommit(true)
        val ps = conn
//            .prepareStatement("upsert into TEST2 (id, ts) select id, TO_TIMESTAMP('2020-08-27 11:11:05') from test2 where id='3' and ts > TO_TIMESTAMP('2020-08-27 11:11:05')")
            .prepareStatement("UPSERT INTO TEST2(ID, ts) VALUES('100', TO_TIMESTAMP('2020-08-27 11:11:06')) ON DUPLICATE KEY IGNORE")
    
        val i = ps.executeUpdate()
        println(i)
        
        
        
        ps.close()
        conn.close()
        
    }
    */
    
    
    def main(args: Array[String]): Unit = {
        val map = Map(1 ->2, 10->20)
        println(map.contains(100))
    }
    
}
