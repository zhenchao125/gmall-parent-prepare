package com.atguigu.gmall.gmalllogger.controll;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/8/9 5:34 下午
 */
@RestController
@Slf4j
public class LoggerController {

    @PostMapping("/applog")
    public String doLog(@RequestBody String logString) {
        // 1. 日志落盘
        saveToDisk(logString);
        // 2. 发送数据到 kafka
        sendToKafka(logString);
        return "ok";
    }

    @Autowired
    KafkaTemplate kafkaTemplate;

    private void sendToKafka(String logString) {
        JSONObject obj = JSON.parseObject(logString);
        // 启动日志和事件日志进入到不同的 topic
        if (obj.getString("start") != null && obj.getString("start").length() > 0) {
            kafkaTemplate.send("gmall_startup_topic", logString);
        } else {
            kafkaTemplate.send("gmall_event_topic", logString);
        }
    }


    private void saveToDisk(String logString) {
        log.info(logString);
    }
}
