package com.atguigu.gmall.gmallpublisher.service

/**
 * Author lzc
 * Date 2020/8/19 10:11 下午
 */
object DSL {
    def getDauDSL(): String =
        """
          |{
          |  "query": {
          |    "match_all": {}
          |  }
          |}
          |""".stripMargin
    
    def getHourDauDSL() = {
        """
          |{
          |  "query": {
          |    "match_all": {}
          |  },
          |  "aggs": {
          |    "group_by_hour": {
          |      "terms": {
          |        "field": "logHour",
          |        "size": 24
          |      }
          |    }
          |  }
          |}
          |""".stripMargin
    }
}
