package com.atguigu.gmall.gmallpublisher2.service;/**
 * Author lzc
 * Date 2020/8/31 6:21 下午
 */

import com.atguigu.gmall.gmallpublisher2.mapper.TrademarkMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/8/31 6:21 下午
 */
@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    TrademarkMapper mapper;

    @Override
    public List<Map<String, Object>>getTmAmount(String startDt, String endDt, int limit) {
        return mapper.getTrademarkSum(startDt, endDt, limit);
    }
}
