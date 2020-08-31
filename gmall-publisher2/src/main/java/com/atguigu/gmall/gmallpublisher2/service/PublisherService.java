package com.atguigu.gmall.gmallpublisher2.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Author lzc
 * Date 2020/8/31 6:18 下午
 */
public interface PublisherService {
    List<Map<String, Object>> getTmAmount(String startDt, String endDt, int limit);
}
