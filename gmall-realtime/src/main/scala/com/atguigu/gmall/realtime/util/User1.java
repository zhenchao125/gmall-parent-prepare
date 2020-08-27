package com.atguigu.gmall.realtime.util;/**
 * Author lzc
 * Date 2020/8/27 3:13 下午
 */

/**
 * @Author lzc
 * @Date 2020/8/27 3:13 下午
 */
public class User1 {
    private String id;
    public String name;

    public User1(){

    }

    public User1(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User1{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
