package com.atguigu.gmall.gmallpublisher2.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Author lzc
 * Date 2020/8/31 6:10 下午
 */
public interface TrademarkMapper {
    // 如果只有一个参数可以不用添加注解, 多个必须添加, 这样在 xml 文件的 sql 中才能获取到参数
    List<Map<String, Object>> getTrademarkSum(@Param("startDt") String startDt, @Param("endDt") String endDt,@Param("limit") int limit);

}
