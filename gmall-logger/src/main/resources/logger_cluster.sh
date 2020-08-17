#!/bin/bash
app=gmall-logger-0.0.1-SNAPSHOT.jar
case $1 in
    "start")
        echo "========在 hadoop102 启动 nginx ==============="
        /opt/module/nginx/sbin/nginx
        for i in hadoop102 hadoop103 hadoop104
        do
            echo "========启动日志服务: $i==============="
            ssh $i  "source /etc/profile ;nohup java -Xms32m -Xmx64m -jar /opt/gmall2020/logger-server/$app >/dev/null 2>&1  &"
        done
     ;;
    "stop")
        echo "========在 hadoop102 停止 nginx ==============="
        /opt/module/nginx/sbin/nginx -s stop
        for i in hadoop102 hadoop103 hadoop104
        do
            echo "========关闭日志服务: $i==============="
            ssh $i "source /etc/profile; jps | grep $app | awk '{print \$2}'|xargs kill" >/dev/null 2>&1
        done
    ;;
    *)
        echo 启动姿势不对, 请使用参数 start 启动日志服务, 使用参数 stop 停止服务
    ;;
esac
