package base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 如果有用户权限问题：-DHADOOP_USER_NAME=hzlimao
 *
 * @author lm
 * @create 2017-12-11 下午3:29
 * 内容：
 * 创建，删除表
 * 创建，更新，删除，查询列簇
 * 新增，更新，删除，查询数据
 **/
public class HbaseTest_1 {

    static Configuration conf = null;
    private static final String ZKconnect = "hadoop05:2181,hadoop07:2181,hadoop07:2181";
//    private static final String ZKconnect="hbase17.lt.163.org:2181,hbase18.lt.163.org:2181,hbase19.lt.163.org:2181";//测试环境

    private static HBaseAdmin admin = null;
    private static HTableDescriptor desc = null;
    private static HTable table = null;
    private static Connection conn = null;

    private static String tableName = "test1_table";
//    private static String tableName = "beauty:Beauty_Channel";//测试环境

    @Before
    public void init() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZKconnect);

        conn = ConnectionFactory.createConnection(conf);

        admin = (HBaseAdmin) conn.getAdmin();
        desc = new HTableDescriptor(TableName.valueOf(tableName));
        table = (HTable) conn.getTable(TableName.valueOf(tableName));
    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (table != null) {
            table.close();
        }


    }

    //创建表
    @org.junit.Test
    public void createTable() throws Exception {
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf2")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf3")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf4")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf5")));
        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在，别瞎输行吗");
        } else {
            admin.createTable(desc);
            System.out.println("表创建成功");
        }
    }

    /**
     * 获取表的信息
     *
     * @throws Exception
     */
    @org.junit.Test
    public void descTable() throws Exception {
        HTableDescriptor desc = table.getTableDescriptor();
        HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();
        for (HColumnDescriptor t : columnFamilies) {
            System.out.println(Bytes.toString(t.getName()));
        }

    }

    /**
     * 操作列簇
     *
     * @throws Exception
     */
    @org.junit.Test
    public void modifyTable() throws Exception {
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf3")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf4")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf5")));
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf6")));
        admin.modifyTable(tableName, desc);

        // 删除该表tableName当中的特定的列簇
        admin.deleteColumn(tableName, "cf6");

        System.out.println("修改成功");

    }

    /**
     * 获取所有表名
     *
     * @throws Exception
     */
    @org.junit.Test
    public void getAllTables() throws Exception {
        String[] tableNames = admin.getTableNames();
        for (int i = 0; i < tableNames.length; i++) {
            System.out.println(tableNames[i]);
        }
    }


    /**
     * 新增数据
     *
     * @throws Exception
     */
    @org.junit.Test
    public void addData() throws Exception {
        Put put = new Put(Bytes.toBytes("r4"));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String nameAsString = columnFamilies[i].getNameAsString();
            if (nameAsString.equals("cf3")) {
                put.addColumn(Bytes.toBytes(nameAsString), Bytes.toBytes("row3"), Bytes.toBytes("4"));
            }
            if (nameAsString.equals("cf4")) {
                put.addColumn(Bytes.toBytes(nameAsString), Bytes.toBytes("row4"), Bytes.toBytes(4));
            }
            if (nameAsString.equals("cf5")) {
                put.addColumn(Bytes.toBytes(nameAsString), Bytes.toBytes("row5"), Bytes.toBytes("name4"));
            }
        }
        table.put(put);
        System.out.println("addData ok!");
    }

    /**
     * 根据rowkey 查询
     *
     * @return
     * @throws Exception
     */
    @org.junit.Test
    public void getResult() throws Exception {
        Get get = new Get(Bytes.toBytes("r1"));

        //指定获取哪些列簇中哪些列
        get.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("row3"));
        get.addColumn(Bytes.toBytes("cf4"), Bytes.toBytes("row4"));

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell), "utf8") + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell), "utf8") + " ");
        }
    }


    /**
     * 遍历查询表
     *
     * @return
     * @throws Exception
     */
    @org.junit.Test
    public void getResultScann() throws Exception {
        Scan scan = new Scan();
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            for (Result result : rs) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell), "utf8") + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell), "utf8") + " ");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 查询范围中的数据(左闭右开)
     */
    @org.junit.Test
    public void scanResult() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("r1"));
        scan.setStopRow(Bytes.toBytes("r4"));
        scan.setMaxVersions(1);
        scan.setCaching(20);
        scan.setBatch(10);
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            //以下是打印内容
            for (Cell cell : result.listCells()) {
                System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell), "utf8") + " ");
                System.out.println("value:" + new String(CellUtil.cloneValue(cell), "utf8") + " ");
            }
        }
    }

    /**
     * 根据条件查询数据
     *
     * @return
     * @throws Exception
     */
    @org.junit.Test
    public void QueryByCondition3() {

        try {

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("cf5"), Bytes
                    .toBytes("row5"), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("name1"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("cf3"), Bytes
                    .toBytes("row3"), CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("1"));

            filters.add(filter2);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                System.out.println("------------->获得到rowkey:" + new String(result.getRow()));
                for (Cell cell : result.listCells()) {
                    System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                    System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell), "utf8") + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell), "utf8") + " ");
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 查询某列数据的某个版本
     *
     * @return
     * @throws Exception
     */
    @org.junit.Test
    public void getResultByVersion() throws Exception {

        Get get = new Get(Bytes.toBytes("r1"));
        get.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("row3"));
        get.setMaxVersions(3);
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell), "utf8") + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell), "utf8") + " ");
        }

    }


    /**
     * 删除指定某列 或 列簇
     *
     * @throws Exception
     */
    @org.junit.Test
    public void deleteColumn() throws Exception {
        Delete de = new Delete(Bytes.toBytes("r1"));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        de.addColumn(Bytes.toBytes("cf5"), Bytes.toBytes("row5"));
        table.delete(de);
    }


    /**
     * 删除指定的某个rowkey
     *
     * @throws Exception
     */
    @org.junit.Test
    public void deleteRowkey() throws Exception {
        Delete de = new Delete(Bytes.toBytes("r1"));
        table.delete(de);
    }

    //让该表失效
    @org.junit.Test
    public void disableTable() throws Exception {
        admin.disableTable(tableName);
    }

    //删除表
    @org.junit.Test
    public void dropTable() throws Exception {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    public static void main(String[] args) {
        while (true) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(2221);
        }
    }

}