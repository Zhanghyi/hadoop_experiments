package cn.zhy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;


public class HBase {

    private static final String TABLE_NAME = "Orders";
    private static final String CF_ORDER_DETAIL = "Order Detail";
    private static final String CF_TRANSACTION = "Transaction";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            //表已经存在先删除再创建
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            //定义表名
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            //增加列族
            table.addFamily(new HColumnDescriptor(CF_ORDER_DETAIL).setCompressionType(Algorithm.NONE));
            table.addFamily(new HColumnDescriptor(CF_TRANSACTION).setCompressionType(Algorithm.NONE));
            createOrOverwrite(admin, table);
        }
    }

    public static void insertData(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             HTable table = (HTable) connection.getTable(TableName.valueOf(TABLE_NAME));) {
            List<Put> PutList = new ArrayList<Put>();

            Put put1 = new Put("00001".getBytes());
            put1.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "41341".getBytes());
            put1.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "1057499".getBytes());
            put1.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "2".getBytes());
            put1.addColumn("Order Detail".getBytes(), "amount".getBytes(), "1".getBytes());
            put1.addColumn("Order Detail".getBytes(), "money".getBytes(), "462.8".getBytes());
            put1.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-4-16 9:21:09".getBytes());
            put1.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-4-16 10:14:47".getBytes());
            put1.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-7-9 2:12:23".getBytes());
            put1.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-7-28 12:10:40".getBytes());
            PutList.add(put1);

            Put put2 = new Put("00002".getBytes());
            put2.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "32805".getBytes());
            put2.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "9203020".getBytes());
            put2.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "4".getBytes());
            put2.addColumn("Order Detail".getBytes(), "amount".getBytes(), "0".getBytes());
            put2.addColumn("Order Detail".getBytes(), "money".getBytes(), "760.3".getBytes());
            put2.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-3-17 0:17:19".getBytes());
            put2.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-3-17 1:15:04".getBytes());
            put2.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-7-4 21:13:27".getBytes());
            put2.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-9-22 6:58:44".getBytes());
            PutList.add(put2);

            Put put3 = new Put("00003".getBytes());
            put3.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "66772".getBytes());
            put3.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "6330669".getBytes());
            put3.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "5".getBytes());
            put3.addColumn("Order Detail".getBytes(), "amount".getBytes(), "9".getBytes());
            put3.addColumn("Order Detail".getBytes(), "money".getBytes(), "97".getBytes());
            put3.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-4-6 11:34:17".getBytes());
            put3.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-4-6 11:37:04".getBytes());
            put3.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-5-10 10:18:01".getBytes());
            put3.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-7-1 8:27:19".getBytes());
            PutList.add(put3);

            Put put4 = new Put("00004".getBytes());
            put4.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "59086".getBytes());
            put4.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "5544997".getBytes());
            put4.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "4".getBytes());
            put4.addColumn("Order Detail".getBytes(), "amount".getBytes(), "4".getBytes());
            put4.addColumn("Order Detail".getBytes(), "money".getBytes(), "550.1".getBytes());
            put4.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-3-26 22:29:44".getBytes());
            put4.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-3-26 22:51:19".getBytes());
            put4.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-7-5 10:09:26".getBytes());
            put4.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-9-13 15:47:28".getBytes());
            PutList.add(put4);

            Put put5 = new Put("00005".getBytes());
            put5.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "94847".getBytes());
            put5.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "5377359".getBytes());
            put5.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "1".getBytes());
            put5.addColumn("Order Detail".getBytes(), "amount".getBytes(), "6".getBytes());
            put5.addColumn("Order Detail".getBytes(), "money".getBytes(), "87.4".getBytes());
            put5.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-4-23 8:43:27".getBytes());
            put5.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-4-23 9:11:33".getBytes());
            put5.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-4-29 11:31:01".getBytes());
            put5.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-5-15 3:24:54".getBytes());
            PutList.add(put5);

            Put put6 = new Put("00006".getBytes());
            put6.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "92140".getBytes());
            put6.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "8739695".getBytes());
            put6.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "5".getBytes());
            put6.addColumn("Order Detail".getBytes(), "amount".getBytes(), "9".getBytes());
            put6.addColumn("Order Detail".getBytes(), "money".getBytes(), "819".getBytes());
            put6.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-2-9 1:03:14".getBytes());
            put6.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-2-9 1:13:04".getBytes());
            put6.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-5-3 0:06:12".getBytes());
            put6.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-8-19 23:38:05".getBytes());
            PutList.add(put6);

            Put put7 = new Put("00007".getBytes());
            put7.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "13851".getBytes());
            put7.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "9503980".getBytes());
            put7.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "9".getBytes());
            put7.addColumn("Order Detail".getBytes(), "amount".getBytes(), "7".getBytes());
            put7.addColumn("Order Detail".getBytes(), "money".getBytes(), "671.3".getBytes());
            put7.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-2-18 15:05:57".getBytes());
            put7.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-2-18 15:57:22".getBytes());
            put7.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-5-8 9:39:56".getBytes());
            put7.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-5-27 3:10:54".getBytes());
            PutList.add(put7);

            Put put8 = new Put("00008".getBytes());
            put8.addColumn("Order Detail".getBytes(), "consumerId".getBytes(), "42138".getBytes());
            put8.addColumn("Order Detail".getBytes(), "itemId".getBytes(), "2682154".getBytes());
            put8.addColumn("Order Detail".getBytes(), "itemCategory".getBytes(), "5".getBytes());
            put8.addColumn("Order Detail".getBytes(), "amount".getBytes(), "3".getBytes());
            put8.addColumn("Order Detail".getBytes(), "money".getBytes(), "11.6".getBytes());
            put8.addColumn("Transaction".getBytes(), "createTime".getBytes(), "2020-2-28 6:54:33".getBytes());
            put8.addColumn("Transaction".getBytes(), "paymentTime".getBytes(), "2020-2-28 7:18:10".getBytes());
            put8.addColumn("Transaction".getBytes(), "deliveryTime".getBytes(), "2020-3-25 22:24:11".getBytes());
            put8.addColumn("Transaction".getBytes(), "CompleteTime".getBytes(), "2020-6-27 10:04:31".getBytes());
            PutList.add(put8);

            //遍历PutList插入数据
            for (Put put : PutList) {
                table.put(put);
            }
        }
    }


    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        createSchemaTables(config);
        insertData(config);
    }
}