package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


/**
 * ClassName: HBaseConnection01
 * Package: com.hbase
 * Description:
 *
 * @Create 2023/9/21 8:33
 * @Version 1.0
 */
public class HBaseConnection01 {

    //声明一个静态属性
    public static Connection connection = null;

    //在静态代码块里将他实现,以后都使用他,来实现类似单例的感觉
    static {

//        //1.创建连接配置对象
//        Configuration conf = new Configuration();
//
//        //2.添加配置对象
//        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        //3.创建连接
        //默认使用同步连接
        try {
            //使用读取本地文件的形式来添加参数
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    //关闭连接
    public static void closeConnection() throws IOException {
        //考虑程序健壮性,判断是否有连接,有再关闭
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {

        //直接使用创建好的连接,不在main线程里面单独创建
        System.out.println(HBaseConnection01.connection);

        //在main线程的最后记得关闭连接
        HBaseConnection01.closeConnection();

    }
}
