package com.atguigu.gmall.gmallpublisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Author lzc
 * Date 2020/8/31 9:48 上午
 */
public interface OrderMapper {
    //1 查询当日交易额总数
    public BigDecimal getOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public List<Map<String, Object>> getOrderAmountHour(String date);
}
