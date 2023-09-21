package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * ClassName: HBaseConnection
 * Package: com.hbase
 * Description:
 *
 * @Create 2023/9/21 8:19
 * @Version 1.0
 */
public class HBaseConnection00 {
    public static void main(String[] args) throws IOException {


        //创建连接配置对象
        Configuration conf = new Configuration();
        //添加配置参数,(zk的地址端口,hbase的地址),我这里写hbase.zookeeper.quorum意思是我在hbase的配置文件的hbase.zookeeper.quorum标签已经配置过了,直接拿过来用
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //创建连接
        //默认使用同步连接
        //这里报错需要抛异常的原因是希望开发者的conf不是在代码中创建的而是应该从resources里的hbase-site.xml读取
        Connection connection = ConnectionFactory.createConnection(conf);
        //可以使用异步连接
        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);

        //使用连接
        System.out.println(connection);


        //关闭连接
        connection.close();
    }
}
