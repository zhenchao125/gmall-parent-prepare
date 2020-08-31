package com.atguigu.gmall.gmallpublisher2.controller;/**
 * Author lzc
 * Date 2020/8/31 6:25 下午
 */

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher2.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Author lzc
 * @Date 2020/8/31 6:25 下午
 */
@RestController
public class PublisherController {
    @Autowired
    public PublisherService service;

    @GetMapping("/trademark")
    public String trademarkStat(String startDt, String endDt, int limit) {
        List<Map<String, Object>> tmAmountList = service.getTmAmount(startDt, endDt, limit);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> map : tmAmountList) {
            Object x = map.get("tm_name");
            Object y = map.get("amount");
            Map<String, Object> ele = new HashMap<>();
            ele.put("x", x);
            ele.put("y", y);
            ele.put("s", 1);
            result.add(ele);
        }


        return JSON.toJSONString(result);


    }
}
