package com.uboxol.hbase;

import com.uboxol.model.HbaseQuery;
import com.uboxol.model.VmMoney;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-29
 * Time: 下午2:40
 * To change this template use File | Settings | File Templates.
 */
public class VmMoneyOperater {
    private final static Logger logger = LoggerFactory.getLogger(VmMoneyOperater.class);

    private static final byte[] POSTFIX = new byte[1];
    public static Configuration conf = HBaseConfiguration.create();
    public static Configuration conf_t = HBaseConfiguration.create();
    static {
//        String path =  Main.class.getClassLoader().getResource("/").getPath();
//        conf.addResource(new Path(Main.class.getClassLoader().getResource("conf/hbase-site.xml").getPath()));
        conf.set("hbase.zookeeper.quorum", "192.168.8.1,192.168.8.2,192.168.8.3");
        conf.set("hbase.zookeeper.property.dataDir", "/usr/local/hbase/zookeeper");
        conf.set("hbase.rootdir", "hdfs://nn01:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");

        conf_t.set("hbase.zookeeper.quorum", "hbase-test");
        conf.set("hbase.rootdir", "file:///usr/local/hbase/data");
        conf.set("hbase.cluster.distributed", "false");
    }

    /**
     * 从正式获取vmmoney记录
     * @param vm
     * @param from
     * @param to
     * @param lastRowKey
     * @param pageCount
     * @return
     */
    public HbaseQuery getVmMoney(String vm,String from,String to,String lastRowKey,int pageCount){
        long start = System.currentTimeMillis();
        HbaseQuery hq = new HbaseQuery();
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();
        HConnection connection =null;
        HTableInterface table = null;

        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable("vm_money_2");
            start = System.currentTimeMillis();
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("data"));

            String fromKey ="9999999_" + (99999999999999L-Long.parseLong(from)+1);
            String toKey = "0_" + (99999999999999L-Long.parseLong(to));

            if (lastRowKey!=null&&!"".equals(lastRowKey)){//设置起始行健
                byte[]  lastRow = Bytes.add(Bytes.toBytes(lastRowKey),POSTFIX);
                scan.setStartRow(lastRow);
            }else{
                scan.setStartRow(Bytes.toBytes(toKey));
            }
            scan.setStopRow(Bytes.toBytes(fromKey));

                Filter pageFilter = new PageFilter(pageCount);
                scan.setFilter(pageFilter);

            scan.setCaching(pageCount);
            ResultScanner s = table.getScanner(scan);
            int count=0;
            String rowKey="";
            for(Result res : s){
                count++;
                VmMoney vmMoney= resutToVmMoney(res);
                rowKey =Bytes.toString(res.getRow());
                vmMoney.setRowKey(rowKey);
                vmMoneyList.add(vmMoney);
            }
            hq.setCount(count);
            hq.setLastKey(rowKey);
            hq.setVmMoneyList(vmMoneyList);
            s.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return hq;
    }

    public void putVmMoney(List<VmMoney> vmmList){
        if (vmmList==null || vmmList.size()==0){
            return;
        }

        long start = System.currentTimeMillis();
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();
        HConnection connection =null;
        HTableInterface table = null;

        try {
            connection = HConnectionManager.createConnection(conf_t);
            table = connection.getTable("vm_money");
            start = System.currentTimeMillis();

            List<Put> putList = new ArrayList<Put>();
            List<Row> batch =new ArrayList<Row>();
            for (VmMoney vmm : vmmList){
                Put p = new Put(Bytes.toBytes(vmm.getRowKey()));
                p.add(Bytes.toBytes("data"),Bytes.toBytes("id"), Bytes.toBytes(vmm.getId()+""));
                p.add(Bytes.toBytes("data"),Bytes.toBytes("inner_code"), Bytes.toBytes(vmm.getInnerCode()));
                p.add(Bytes.toBytes("data"),Bytes.toBytes("node_id"), Bytes.toBytes(vmm.getNodeId()+""));
                p.add(Bytes.toBytes("data"),Bytes.toBytes("pay_type"), Bytes.toBytes(vmm.getPayType()+""));
                p.add(Bytes.toBytes("data"),Bytes.toBytes("cts"), Bytes.toBytes(vmm.getCts()));
//                putList.add(p);
                batch.add(p);
            }

            //插入
            Object[] result = new Object[batch.size()];
//            table.put(putList);
            table.batch(batch,result);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    public List<VmMoney> getVmMoneyOne(String key){
        long start = System.currentTimeMillis();
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();
        HConnection connection =null;
        HTableInterface table = null;

        try {
            connection = HConnectionManager.createConnection(conf_t);
            table = connection.getTable("vm_money");
            Get get =new Get(Bytes.toBytes(key));
            get.addFamily(Bytes.toBytes("data"));
            Result result = table.get(get);
            System.out.println(result.toString());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return vmMoneyList;
    }

    /**
     * 获取指定行健对应记录
     * @param rowKeys
     * @return
     */
    public List<VmMoney> getVmMoneyByRowKey(List<String> rowKeys){
        if (rowKeys==null|| rowKeys.size()==0){
            return null;
        }
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();

        HConnection connection =null;
        HTableInterface table = null;

        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable("vm_money_2");

            List<Get> getList = new ArrayList<Get>();
            for (String rowKey:rowKeys){
                Get get =new Get(Bytes.toBytes(rowKey));
                get.addFamily(Bytes.toBytes("data"));
                getList.add(get);
            }

            Result[] result= table.get(getList);
            for(Result res :result){
                VmMoney vmMoney= resutToVmMoney(res);
                vmMoneyList.add(vmMoney);
            }
        }   catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return vmMoneyList;
    }

    private VmMoney resutToVmMoney(Result result){
        byte[] innerCode = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("inner_code"));
        byte[] cts = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("cts"));
        byte[] nodeId = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("node_id"));
        byte[] id = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("id"));
        byte[] payType = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("pay_type"));

        VmMoney vmMoney = new VmMoney();
        try {
            //判断查询类型
            if (payType.length != Bytes.SIZEOF_INT ||Bytes.SIZEOF_INT>payType.length) {   //字段非int型
                if(innerCode!=null){
                    vmMoney.setInnerCode(new String(innerCode));
                }
                if(nodeId!=null){
                    vmMoney.setNodeId(new Integer(Bytes.toString(nodeId)));
                }
                if(cts!=null){
                    vmMoney.setCts(new String(cts));
                }
                if(id!=null){
                    vmMoney.setId(new Integer(Bytes.toString(id)));
                }
                if(payType!=null){
                    vmMoney.setPayType(new Integer(Bytes.toString(payType)));
                }
            } else{
                if(innerCode!=null){
                    vmMoney.setInnerCode(new String(innerCode));
                }
                if(nodeId!=null){
                    vmMoney.setNodeId(Bytes.toInt(nodeId));
                }
                if(cts!=null){
                    vmMoney.setCts(new String(cts));
                }
                if(id!=null){
                    vmMoney.setId(Bytes.toInt(id));
                }
                if(payType!=null){
                    vmMoney.setPayType(Bytes.toInt(payType));
                }
            }
        }
        catch (Exception e) {
            logger.error(e.toString());
        }

        return vmMoney;
    }
}
