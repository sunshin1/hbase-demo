package com.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * ClassName: HBaseDDL
 * Package: com.hbase
 * Description:
 *
 * @Create 2023/9/21 8:55
 * @Version 1.0
 */
public class HBaseDDL {

    //我们前面提到HBase的连接是一个重量级的,所以为了减少创建,我们直接调用前面创建好的连接
    //声明一个静态属性,获取我们之前创建的连接
    public static Connection connection = HBaseConnection01.connection;


    /**
     * 创建命名空间
     * @param namespace 命名空间的名称
     */
    public static void createNamespace(String namespace) throws IOException {
        //点击connection查看源码我们发现作者将所有的操作都封装在了table和admin这两个里面,table是操作表dml,admin是操作元数据ddl
        //1.获取admin
        //此处的异常先不要抛出,等待方法写完,再统一进行处理
        //admin的连接是轻量级的,不是线程安全的,不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();

        //2.调用方法创建命名空间
        //代码相对shell更加底层,所以shell能够实现的功能,代码一定能实现
        //所以需要填写完整的命名空间描述

        //2.1创建命名空间描述建造者 => 设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);

        //2.2给命名空间添加需求
        builder.addConfiguration("user","sunshine");

        //2.3使用builder构造出对应的添加完参数的对象 完成创建
        //创建命名空间出现的问题,都属于本方法自身的问题,不应该抛出
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
            e.printStackTrace();
        }

        //3.关闭admin
        admin.close();
    }


    /**
     * 判断表格是否存在
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true存在 false不存在
     */
    public static boolean isTableExists(String namespace,String tableName) throws IOException {

        //1.获取Admin
        Admin admin = connection.getAdmin();

        //2.使用方法判断表格是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }


        //关闭admin
        admin.close();


        //3.返回结果
        return b;


    }

    /**
     * 创建表格
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @param columnFamilies 列组名称 可以有多个
     */
    public static void createTable(String namespace,String tableName,String... columnFamilies) throws IOException {
        //判断是否至少有一个列族
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }

        //判断表格是否存在
        if (isTableExists(namespace,tableName)) {
            System.out.println("表格已经存在");
            return;
        }

        //1.获取admin
        Admin admin = connection.getAdmin();

        //2.调用方法创建表格
        //2.1创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        //2.2添加参数
        for (String columnFamily : columnFamilies) {
            //2.3创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            //2.4对应当前的列族添加参数
            //添加版本参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            //2.5创建添加完整参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        //2.3创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("表格已经存在");
        }

        //3.关闭admin
        admin.close();
    }

    /**
     * 修改表格中一个列族的版本
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param columnFamily 列族名称
     * @param version 版本
     */
    public static void modifyTable(String namespace,String tableName,String columnFamily,int version) throws IOException {

        //判断表格是否存在
        if (!isTableExists(namespace,tableName)) {
            System.out.println("表格不存在");
            return;
        }



        //获取admin
        Admin admin = connection.getAdmin();



        try {

            //2.调用方法修改表格
            //2.0获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            //2.1创建一个表格描述建造者
            //如果使用填写tablename的方法,相当于创建了一个新的表格描述者 没有之前的信息
            //如果想要修改之前的信息  必须调用方法填写一个旧的表格描述信息
            TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(descriptor);

            //2.2对应建造者进行表格数据的修改
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));

            //创建列族描述建造者
            //需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);

            //修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);

            //此处修改的时候,如果填写的新创建 那么别的参数会初始化
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());



            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //关闭admin
        admin.close();
    }

    /**
     * 删除表格
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true删除成功
     */
    public static boolean deleTable(String namespace,String tableName) throws IOException {
        //1.判断表格是否存在
        if (!isTableExists(namespace,tableName)) {
            System.out.println("表格不存在,无法删除");
            return false;
        }

        //2.获取admin
        Admin admin = connection.getAdmin();

        //3.调用相关的方法删除表格
        try {
            //hbase删除表格之前,一定要先标记表格为不可用
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //关闭admin
        admin.close();
        return  true;
    }


    public static void main(String[] args) throws IOException {

        //测试创建命名空间
        //应该先保证连接没有问题,再来调用相关的方法
//        createNamespace("sunshine");

        //测试表格是否存在
//        System.out.println(isTableExists("bigdata", "student"));

        //测试创建表格
//        createTable("sunshine","student","info","msg");
//        createTable("sunshine","class");

        //测试修改表格
//        modifyTable("sunshine","student","info",6);

        //测试删除表格
        System.out.println(deleTable("sunshine", "student"));


        //其它代码
        System.out.println("其它代码");

        //关闭
        HBaseConnection01.closeConnection();
    }
}
