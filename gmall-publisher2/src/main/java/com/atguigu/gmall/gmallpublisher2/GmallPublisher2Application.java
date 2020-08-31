package com.atguigu.gmall.gmallpublisher2;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.gmallpublisher2.mapper")
public class GmallPublisher2Application {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisher2Application.class, args);
    }

}
