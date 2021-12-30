# 大数据实验



## 实验一 HBase

### 环境配置

- os: centos 7 (腾讯云主机)
- java: 1.8
- hadoop：2.10.1
- hbase: 2.3.7

**1.** 安装jdk
```bash
[zhy@VM-24-10-centos ~]$ sudo yum install java-1.8.0-openjdk
[zhy@VM-24-10-centos ~]$ sudo yum install java-1.8.0-openjdk-devel
```

**2.** 下载并且运行hadoop （伪分布式）管理页面 http://82.156.172.44:50070/
```bash
[zhy@VM-24-10-centos ~]$ wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz
[zhy@VM-24-10-centos ~]$ tar -xvzf hadoop-2.10.1.tar.gz
[zhy@VM-24-10-centos hadoop-2.10.1]$ sbin/start-dfs.sh
```
**3.**  下载并且运行hbase （伪分布式） 管理页面 http://82.156.172.44:16010/

```bash
[zhy@VM-24-10-centos ~]$ wget https://mirrors.tuna.tsinghua.edu.cn/apache/hbase/2.3.7/hbase-2.3.7-bin.tar.gz
[zhy@VM-24-10-centos ~]$ tar -xvzf hbase-2.3.7-bin.tar.gz
[zhy@VM-24-10-centos hbase-2.3.7]$ bin/start-hbase.sh
```

### 程序概述
先定义表名和列族建表，然后插入数据。

### 程序细节

建表

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

插数据

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
            
            ...
            
            //遍历PutList插入数据
            for (Put put : PutList) {
                table.put(put);
            }
        }
    }


### 程序测试

```bash
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop jar ~/hbase-1.0-SNAPSHOT-jar-with-dependencies.jar cn.zhy.hadoop.HBase
```


可以在该页面查看建表结果:

http://82.156.172.44:16010/tablesDetailed.jsp

hbase shell中使用scan命令查询表中全部数据
```bash
hbase(main):001:0> scan 'Orders'
ROW COLUMN+CELL
00001 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.727, value=1
00001 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.727, value=41341
00001 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.727, value=2
00001 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.727, value=1057499
00001 column=Order Detail:money, timestamp=2021-12-31T01:19:09.727, value=462.8
00001 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.727, value=2020-7-28 12:10:40
00001 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.727, value=2020-4-16 9:21:09
00001 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.727, value=2020-7-9 2:12:23
00001 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.727, value=2020-4-16 10:14:47
00002 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.734, value=0
00002 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.734, value=32805
00002 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.734, value=4
00002 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.734, value=9203020
00002 column=Order Detail:money, timestamp=2021-12-31T01:19:09.734, value=760.3
00002 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.734, value=2020-9-22 6:58:44
00002 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.734, value=2020-3-17 0:17:19
00002 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.734, value=2020-7-4 21:13:27
00002 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.734, value=2020-3-17 1:15:04
00003 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.738, value=9
00003 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.738, value=66772
00003 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.738, value=5
00003 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.738, value=6330669
00003 column=Order Detail:money, timestamp=2021-12-31T01:19:09.738, value=97
00003 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.738, value=2020-7-1 8:27:19
00003 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.738, value=2020-4-6 11:34:17
00003 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.738, value=2020-5-10 10:18:01
00003 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.738, value=2020-4-6 11:37:04
00004 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.743, value=4
00004 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.743, value=59086
00004 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.743, value=4
00004 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.743, value=5544997
00004 column=Order Detail:money, timestamp=2021-12-31T01:19:09.743, value=550.1
00004 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.743, value=2020-9-13 15:47:28
00004 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.743, value=2020-3-26 22:29:44
00004 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.743, value=2020-7-5 10:09:26
00004 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.743, value=2020-3-26 22:51:19
00005 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.747, value=6
00005 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.747, value=94847
00005 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.747, value=1
00005 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.747, value=5377359
00005 column=Order Detail:money, timestamp=2021-12-31T01:19:09.747, value=87.4
00005 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.747, value=2020-5-15 3:24:54
00005 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-23 8:43:27
00005 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-29 11:31:01
00005 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-23 9:11:33
00006 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.750, value=9
00006 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.750, value=92140
00006 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.750, value=5
00006 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.750, value=8739695
00006 column=Order Detail:money, timestamp=2021-12-31T01:19:09.750, value=819
00006 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.750, value=2020-8-19 23:38:05
00006 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.750, value=2020-2-9 1:03:14
00006 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.750, value=2020-5-3 0:06:12
00006 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.750, value=2020-2-9 1:13:04
00007 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.753, value=7
00007 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.753, value=13851
00007 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.753, value=9
00007 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.753, value=9503980
00007 column=Order Detail:money, timestamp=2021-12-31T01:19:09.753, value=671.3
00007 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.753, value=2020-5-27 3:10:54
00007 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.753, value=2020-2-18 15:05:57
00007 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.753, value=2020-5-8 9:39:56
00007 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.753, value=2020-2-18 15:57:22
00008 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.756, value=3
00008 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.756, value=42138
00008 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.756, value=5
00008 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.756, value=2682154
00008 column=Order Detail:money, timestamp=2021-12-31T01:19:09.756, value=11.6
00008 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.756, value=2020-6-27 10:04:31
00008 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.756, value=2020-2-28 6:54:33
00008 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.756, value=2020-3-25 22:24:11
00008 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.756, value=2020-2-28 7:18:10
8 row(s)
Took 0.6416 seconds
```


## 实验二 HotWords

### 环境配置

同实验一 略

### 程序概述

统计文本当中频率最高的单词分为两个步骤：
第一步通过词频统计，输出文本中每个单词出现的次数，即WordCount。 第二步，输入每个单词出现的次数，得到次数最多的K个单词，即TopK。# 大数据实验



## 实验一 HBase

### 环境配置

- os: centos 7 (腾讯云主机)
- java: 1.8
- hadoop：2.10.1
- hbase: 2.3.7

**1.** 安装jdk
```bash
[zhy@VM-24-10-centos ~]$ sudo yum install java-1.8.0-openjdk
[zhy@VM-24-10-centos ~]$ sudo yum install java-1.8.0-openjdk-devel
```

**2.** 下载并且运行hadoop （伪分布式）管理页面 http://82.156.172.44:50070/
```bash
[zhy@VM-24-10-centos ~]$ wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz
[zhy@VM-24-10-centos ~]$ tar -xvzf hadoop-2.10.1.tar.gz
[zhy@VM-24-10-centos hadoop-2.10.1]$ sbin/start-dfs.sh
```
**3.**  下载并且运行hbase （伪分布式） 管理页面 http://82.156.172.44:16010/

```bash
[zhy@VM-24-10-centos ~]$ wget https://mirrors.tuna.tsinghua.edu.cn/apache/hbase/2.3.7/hbase-2.3.7-bin.tar.gz
[zhy@VM-24-10-centos ~]$ tar -xvzf hbase-2.3.7-bin.tar.gz
[zhy@VM-24-10-centos hbase-2.3.7]$ bin/start-hbase.sh
```

### 程序概述
先定义表名和列族建表，然后插入数据。

### 程序细节

建表

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

插数据

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
            
            ...
            
            //遍历PutList插入数据
            for (Put put : PutList) {
                table.put(put);
            }
        }
    }


### 程序测试

```bash
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop jar ~/hbase-1.0-SNAPSHOT-jar-with-dependencies.jar cn.zhy.hadoop.HBase
```


可以在该页面查看建表结果:

http://82.156.172.44:16010/tablesDetailed.jsp

hbase shell中使用scan命令查询表中全部数据
```bash
hbase(main):001:0> scan 'Orders'
ROW COLUMN+CELL
00001 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.727, value=1
00001 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.727, value=41341
00001 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.727, value=2
00001 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.727, value=1057499
00001 column=Order Detail:money, timestamp=2021-12-31T01:19:09.727, value=462.8
00001 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.727, value=2020-7-28 12:10:40
00001 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.727, value=2020-4-16 9:21:09
00001 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.727, value=2020-7-9 2:12:23
00001 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.727, value=2020-4-16 10:14:47
00002 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.734, value=0
00002 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.734, value=32805
00002 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.734, value=4
00002 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.734, value=9203020
00002 column=Order Detail:money, timestamp=2021-12-31T01:19:09.734, value=760.3
00002 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.734, value=2020-9-22 6:58:44
00002 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.734, value=2020-3-17 0:17:19
00002 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.734, value=2020-7-4 21:13:27
00002 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.734, value=2020-3-17 1:15:04
00003 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.738, value=9
00003 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.738, value=66772
00003 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.738, value=5
00003 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.738, value=6330669
00003 column=Order Detail:money, timestamp=2021-12-31T01:19:09.738, value=97
00003 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.738, value=2020-7-1 8:27:19
00003 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.738, value=2020-4-6 11:34:17
00003 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.738, value=2020-5-10 10:18:01
00003 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.738, value=2020-4-6 11:37:04
00004 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.743, value=4
00004 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.743, value=59086
00004 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.743, value=4
00004 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.743, value=5544997
00004 column=Order Detail:money, timestamp=2021-12-31T01:19:09.743, value=550.1
00004 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.743, value=2020-9-13 15:47:28
00004 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.743, value=2020-3-26 22:29:44
00004 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.743, value=2020-7-5 10:09:26
00004 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.743, value=2020-3-26 22:51:19
00005 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.747, value=6
00005 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.747, value=94847
00005 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.747, value=1
00005 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.747, value=5377359
00005 column=Order Detail:money, timestamp=2021-12-31T01:19:09.747, value=87.4
00005 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.747, value=2020-5-15 3:24:54
00005 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-23 8:43:27
00005 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-29 11:31:01
00005 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.747, value=2020-4-23 9:11:33
00006 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.750, value=9
00006 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.750, value=92140
00006 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.750, value=5
00006 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.750, value=8739695
00006 column=Order Detail:money, timestamp=2021-12-31T01:19:09.750, value=819
00006 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.750, value=2020-8-19 23:38:05
00006 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.750, value=2020-2-9 1:03:14
00006 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.750, value=2020-5-3 0:06:12
00006 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.750, value=2020-2-9 1:13:04
00007 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.753, value=7
00007 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.753, value=13851
00007 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.753, value=9
00007 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.753, value=9503980
00007 column=Order Detail:money, timestamp=2021-12-31T01:19:09.753, value=671.3
00007 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.753, value=2020-5-27 3:10:54
00007 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.753, value=2020-2-18 15:05:57
00007 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.753, value=2020-5-8 9:39:56
00007 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.753, value=2020-2-18 15:57:22
00008 column=Order Detail:amount, timestamp=2021-12-31T01:19:09.756, value=3
00008 column=Order Detail:consumerId, timestamp=2021-12-31T01:19:09.756, value=42138
00008 column=Order Detail:itemCategory, timestamp=2021-12-31T01:19:09.756, value=5
00008 column=Order Detail:itemId, timestamp=2021-12-31T01:19:09.756, value=2682154
00008 column=Order Detail:money, timestamp=2021-12-31T01:19:09.756, value=11.6
00008 column=Transaction:CompleteTime, timestamp=2021-12-31T01:19:09.756, value=2020-6-27 10:04:31
00008 column=Transaction:createTime, timestamp=2021-12-31T01:19:09.756, value=2020-2-28 6:54:33
00008 column=Transaction:deliveryTime, timestamp=2021-12-31T01:19:09.756, value=2020-3-25 22:24:11
00008 column=Transaction:paymentTime, timestamp=2021-12-31T01:19:09.756, value=2020-2-28 7:18:10
8 row(s)
Took 0.6416 seconds
```


## 实验二 HotWords

### 环境配置

同实验一 略

### 程序概述

统计文本当中频率最高的单词分为两个步骤：
第一步通过词频统计，输出文本中每个单词出现的次数，即WordCount。 第二步，输入每个单词出现的次数，得到次数最多的K个单词，即TopK。

#### WordCount

- mapper读取原始文本，进行分词，输出<单词，1>
- reducer对相同的单词进行累加，输出<单词，单词总出现次数>

#### TopK

- mapper读取wordcount的结果<单词，单词总出现次数>，同时在每次处理的时候在内存中保留前200频率的单词。在map结束的阶段将前200频率的单词按照<单词，单词总出现次数>输出。
- reducer与mapper的逻辑处理相同，不同之处在于reducer的输入是mapper处理过的所有单词的子集中前200频率的单词，输出的是所有单词中前200频率的单词。

### 程序细节

#### WordCount

解析停词表

        private void parseSkipFile(String fileName) {
            try {
                //读取停词表文件，按行解析，写入stopWords中
                fis = new BufferedReader(new FileReader(fileName));
                String stopWord = null;
                while ((stopWord = fis.readLine()) != null) {
                    stopWords.add(stopWord);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

map

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            //替换掉非字母，即标点符号不会出现在结果当中
            line = line.replaceAll("[^a-zA-Z']", " ");

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String tmpWord = itr.nextToken();
                //匹配停词表，当单词不在停词表中 则输出到结果中
                if (!stopWords.contains(tmpWord)) {
                    word.set(tmpWord);
                    context.write(word, one);
                    Counter counter = context.getCounter(CountersEnum.class.getName(),
                            CountersEnum.INPUT_WORDS.toString());
                    counter.increment(1);
                }
            }
        }

reduce

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            //相同的单词累加计数
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

TopK

map/reduce逻辑相同

    public static class TopKMapper extends Mapper<Text, Text, Text, IntWritable> {
        //保存频率前K个的单词和单词的频率
        private List<TwoTuple<Text, IntWritable>> topk = new ArrayList<TwoTuple<Text, IntWritable>>();
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
            Text kk = new Text();
            kk.set(key);
            IntWritable vv = new IntWritable(Integer.parseInt(value.toString()));

            //遍历当前的topK, 看是否要将单词插入topK当中， 下面2种情况要加入
            // topK中不足K个
            // 当前单词大于topK中频率最小的单词
            for (int i = 0; i < topk.size(); i++) {
                if (vv.compareTo(topk.get(i).count) > 0) {
                    topk.add(i, new TwoTuple(kk, vv));
                    break;
                } else if (i == topk.size() - 1 && i < k - 1) {
                    topk.add(new TwoTuple(kk, vv));
                    break;
                }  else {
                    continue;
                }
            }
            if (topk.size() == 0) {
                topk.add(new TwoTuple(kk, vv));
            } else if (topk.size() > k) {
                topk.remove(k);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            //结束的时候把频率前K的单词输出
            for (TwoTuple<Text, IntWritable> tt : topk) {
                context.write(tt.word, tt.count);
            }
        }
    }

### 程序测试

在hdfs新建文件夹 并且把输入文件和停词文件从文件系统放到hdfs的hot_words/input目录下

```bash
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir /user
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir /user/zhy
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir hot_words
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir hot_words/input
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -put ~/text.txt hot_words/input
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -put ~/stop_words.txt hot_words/input
```

运行第一个步骤wordcount
```bash

[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop jar ~/hot-words-1.0-SNAPSHOT-jar-with-dependencies.jar cn.zhy.hadoop.WordCount -Dwordcount.case.sensitive=false hot_words/input/text.txt hot_words/wordcount_output -skip hot_words/input/stop_words.txt
```

运行第二个步骤topk, 把第一步的结果作为输入
```bash

[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop jar ~/hot-words-1.0-SNAPSHOT-jar-with-dependencies.jar cn.zhy.hadoop.TopK hot_words/wordcount_output/part-r-00000 hot_words/topk_output
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -cat hot_words/topk_output/part-r-00000
```


输出词频前200的单词如下：


edward 2898 eyes 2218  jacob 1866 bella 1781 alice 1414 time 1400  voice 1383 head 1169  charlie 1100   looked 1038    hand 992   carlisle 819   feel 625   door 588   jasper 580 hands 577  stared 562 told 539   expression 532 human 531  moment 526 hard 523   sighed 523 hear 514   whispered 508  mind 501   emmett 496 smiled 486 arms 480   lit 476    heard 475  left 475   sound 452  amber 445  abc 440    abclit 440 converter 440  processtext 440    generatedby 439    suddenly 438   rosalie 435    lips 434   smile 430  life 421   day 420    sam 414    car 403    pulled 403 wrong 403  laughed 401    house 397  black 394  breath 391 hair 390   night 390  skin 385   teeth 378  dark 375   seth 369   mike 367   body 357   started 357    pain 347   renesmee 347   close 343  love 343   deep 335   shook 334  waiting 334    edward's 333   held 331   thinking 330   answer 326 arm 325    jake 321   hurt 317   leave 308  jessica 304    walked 303 aro 301    girl 301   realized 301   slowly 301 cold 300   feet 298   nodded 297 family 296 coming 294 idea 294   esme 292   people 291 vampire 291    blood 290  bad 289    fine 287   light 283  stay 283   meyer 280  murmured 275   muttered 275   stephenie 275  tone 274   wondered 271   finally 269    bit 268    understand 263 waited 263 billy 260  question 254   guess 251  watched 251    spoke 248  surprised 248  answered 246   heart 243  matter 243 school 243 called 241 fingers 241    agreed 240 caught 239 white 239  talk 238   truck 238  fight 237  easy 236   minute 236 sat 236    lot 234    mouth 234  remember 233   staring 230    friends 229    yeah 229   word 228   sense 227  leah 226   rest 226   table 225  throat 225 attention 217  continued 216  reason 216 change 215 chest 215  sounded 215    happy 211  leaned 211 moved 211  worry 210  meant 208  quiet 208  volturi 208    wait 207   promised 206   air 205    wide 203   forest 202 scent 202  afraid 201 edge 201   kill 201   dad 200    strange 198    care 195   morning 195    perfect 195    changed 194    cullens 194    reached 193    watching 193   nice 191   feeling 190    shoulder 190   window 190 sleep 189  tonight 189    vampires 189   stopped 188    carefully 187  control 187    pretty 185 wolf 185   lost 184   happened 183   reminded 182   beautiful 181  demanded 181   silence 181    warm 181   worried 181    angela 180 frowned 180    rolled 180 set 180    chance 179 floor 179  pack 179   hold 174   stood 174




## 实验三 K-Means

### 环境配置

同实验一 略

### 程序概述

KMeans算法首先初始化K个中心点，在每次mapreduce迭代中不断更新中心点，KMeans算法的停止条件是每个中心点的变化量小于一个阈值，或者迭代次数达到了最大次数。
mapper每次读入一行数据点，计算该行与所有中心点的距离，求出离该数据点最近的中心节点，输出<中心点ID，数据点坐标>。
reducer计算具有相同中心点的数据点坐标的平均值，并将该新的中心点输出。


### 程序细节

Point 实现 org.apache.hadoop.io.Writable 作为数据点坐标的数据结构

    //计算点之间的距离
    public float distance(Point p, int h) {
        ...
    }

    //计算节点平均值 用于更新中心节点
    public void average() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float) Math.round(temp * 100000) / 100000.0f;
        }
        this.numPoints = 1;
    }


map


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] pointString = value.toString().split(" ");
            point.set(pointString);

            float minDist = Float.POSITIVE_INFINITY;
            float distance = 0.0f;
            int nearest = -1;

            //计算该数据点最近的中心节点
            for (int i = 0; i < centroids.length; i++) {
                distance = point.distance(centroids[i], p);
                if (distance < minDist) {
                    nearest = i;
                    minDist = distance;
                }
            }

            //写入<中心点ID，数据点坐标>
            centroid.set(nearest);
            context.write(centroid, point);
        }

reduce


        public void reduce(IntWritable centroid, Iterable<Point> partialSums, Context context)
                throws IOException, InterruptedException {

            //计算具有相同中心点的数据点坐标的平均值
            Point sum = Point.copy(partialSums.iterator().next());
            while (partialSums.iterator().hasNext()) {
                sum.sum(partialSums.iterator().next());
            }
            sum.average();


            //把数据点平均值作为新的中心点写入结果当中<中心点ID, 中心点坐标>
            centroidId.set(centroid.toString());
            centroidValue.set(sum.toString());
            context.write(centroidId, centroidValue);
        }




### 程序测试

在hdfs新建文件夹 并且把输入文件放到hdfs的kmeans/input目录下

```bash
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir kmeans
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -mkdir kmeans/input
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop fs -put ~/kmeans_input/* kmeans/input
```

执行KMeans任务
```bash
[zhy@VM-24-10-centos hadoop-2.10.1]$ ./bin/hadoop jar ~/k-means-1.0-SNAPSHOT-jar-with-dependencies.jar cn.zhy.hadoop.KMeans kmeans/input kmeans/output
```

生成中心点坐标（本次任务取K=4）
```
-1033.41951909 146.42315114
151.52158012 -643.98751618
589.81437591 -933.42158007
386.51295242 203.74213957
```



使用python对结果进行可视化，即通过原始的数据点坐标和mapreduce产生的中心点计算每个数据点最近的中心点，作为该数据点的聚类结果，聚类结果如下图所示：

![聚类结果](https://raw.githubusercontent.com/Zhanghyi/hadoop_experiments/main/k-means/kmeans.png "聚类结果")


