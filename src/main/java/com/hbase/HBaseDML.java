package com.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ClassName: HBaseDML
 * Package: com.hbase
 * Description:
 *
 * @Create 2023/9/21 12:31
 * @Version 1.0
 */
public class HBaseDML {
    //静态属性
    public static Connection connection = HBaseConnection01.connection;


    /**
     * 插入数据
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @param rowKey    主键
     * @param columnFamily  列族名称
     * @param columnName    列名
     * @param value     值
     */
    public static void putCell(String namespace,String tableName,
                               String rowKey,String columnFamily,String columnName,String value)
            throws IOException {
        //暂时后面就不再判断表格是否存在这种事了,仅实现功能

        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2.调用相关方法插入数据
        //2.1创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //2.2给put对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        //2.3将对象写入相应的方法
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //关闭table
        table.close();
    }


    /**
     * 读取数据 读取对应的一行的某一列
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param rowKey    主键
     * @param columnFamily  列族名称
     * @param columnName    列名
     */
    public static void getCells(String namespace,String tableName,
                                String rowKey,String columnFamily,String columnName)
            throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2.获取get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //如果直接调用get方法读取数据   此时读一整行数据
        //如果想读取某一列的数据   需要添加对应的参数
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));


        //设置读取数据的版本
        get.readAllVersions();


        try {
            //读取数据 得到result对象
            Result result =  table.get(get);
            //处理数据
            Cell[] cells = result.rawCells();

            //测试方法:直接把读取的数据打印到控制台
            //如果是实际开发   需要再额外写方法    对应处理数据
            for (Cell cell : cells) {
                //cell存储数据比较底层  cell.getValueArray()读出来的数据是乱码
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        //关闭table
        table.close();
    }


    /**
     * 扫描数据(读取多行数据)
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param startRow  开始的row(包含)
     * @param stopRow   结束的row(不包含)
     */
    public static void scanRows(String namespace,String tableName,
                                String startRow,String stopRow) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));


        //2.创建scan对象
        Scan scan = new Scan();
        //如果此时直接调用 会直接扫描整张表

        //添加参数 来控制扫描的数据
        //默认包含,可在填写true或false参数来选择是否包含
        scan.withStartRow(Bytes.toBytes(startRow));
        //默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));

        //读取多行数据    获取scanner

        try {
            ResultScanner scanner = table.getScanner(scan);

            //result来记录一行数据         cell数组
            //ResultScanner来记录多行数据  result数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(
                            new String(CellUtil.cloneRow(cell))
                                    + "-"
                                    + new String(CellUtil.cloneFamily(cell))
                                    + "-"
                                    + new String(CellUtil.cloneQualifier(cell))
                                    + "-"
                                    +new String(CellUtil.cloneValue(cell))
                                    +"\t");
                }

                System.out.println();
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        //关闭table
        table.close();

    }


    /**
     * 带过滤的扫描
     * @param namespace 命名空间
     * @param tableName 表名
     * @param startRow  开始row
     * @param stopRow   结束row     * @param columnFamily  列族名
     * @param columnName    列名
     * @param value     值
     */
    public static void filterScan(String namespace,String tableName,String startRow,
                                  String stopRow,String columnFamily,String columnName,
                                  String value) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));


        //2.创建scan对象
        Scan scan = new Scan();
        //如果此时直接调用 会直接扫描整张表

        //添加参数 来控制扫描的数据
        //默认包含,可在填写true或false参数来选择是否包含
        scan.withStartRow(Bytes.toBytes(startRow));
        //默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));



        try {

            //可以添加多个过滤
            FilterList filterList = new FilterList();

            //创建过滤器
            //(1)结果只保留当前列的数据
            ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                    //列族名称
                    Bytes.toBytes(columnFamily),
                    //列名
                    Bytes.toBytes(columnName),
                    //比较关系
                    CompareOperator.EQUAL,
                    //值
                    Bytes.toBytes(value)
            );

            //(2)结果保留整行数据
            //结果同时会保留没有没有当前列的数据,这个过滤原理是谁不满足移除谁,但没有当前列无法比较就保留下来
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    //列族名称
                    Bytes.toBytes(columnFamily),
                    //列名
                    Bytes.toBytes(columnName),
                    //比较关系
                    CompareOperator.EQUAL,
                    //值
                    Bytes.toBytes(value)
            );

            //本身可以保留多个过滤器,但数据只会越过滤越少
            filterList.addFilter(singleColumnValueFilter);

            //添加过滤
            scan.setFilter(filterList);

            //读取多行数据    获取scanner




            ResultScanner scanner = table.getScanner(scan);
            //result来记录一行数据         cell数组
            //ResultScanner来记录多行数据  result数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(
                            new String(CellUtil.cloneRow(cell))
                                    + "-"
                                    + new String(CellUtil.cloneFamily(cell))
                                    + "-"
                                    + new String(CellUtil.cloneQualifier(cell))
                                    + "-"
                                    +new String(CellUtil.cloneValue(cell))
                                    +"\t");
                }

                System.out.println();
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        //关闭table
        table.close();


    }


    /**
     * 删除某一行的某一列
     * @param namespace 命名空间
     * @param tableName 表名
     * @param rowKey 主键
     * @param columnFamily 列族
     * @param columnName 列名
     */
    public static void deleteColumn(String namespace,String tableName,String rowKey,
                                    String columnFamily,String columnName) throws IOException {
        //1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));



        try {
            //2.创建delete对象
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            //添加列信息
            //addColumn删除一个版本
//            delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
            //addColumns删除所有版本
            //按照逻辑需要删除所有版本的数据
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //3.关闭table
        table.close();

    }

    public static void main(String[] args) throws IOException {

//        //测试添加数据
//        putCell("sunshine","student","2001",
//                "info","name","zhangsan");
//        putCell("sunshine","student","2001",
//                "info","name","lisi");
//        putCell("sunshine","student","2001",
//                "info","name","wangwu");

//        //测试读取数据
//        getCells("sunshine","student",
//                "2001", "info","name");

        //测试读取多行数据
//        scanRows("bigdata","student","1001","1004");

//        //测试有过滤的读取数据
//        filterScan("bigdata","student",
//                "1001","1004",
//                "info","age","18");

        //测试删除数据
        //删除前读取
        getCells("bigdata","student",
                "1002", "info","name");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        //删除
        deleteColumn("bigdata","student",
                "1002", "info","name");
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        //删除后读取
        getCells("bigdata","student",
                "1002", "info","name");


        //其它代码
        System.out.println("其它代码");

        //关闭连接
        HBaseConnection01.closeConnection();
    }
}
