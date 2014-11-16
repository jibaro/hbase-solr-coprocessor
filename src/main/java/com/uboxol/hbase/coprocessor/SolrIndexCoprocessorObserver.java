package com.uboxol.hbase.coprocessor;

import com.uboxol.model.VmMoney;
import com.uboxol.solr.SolrWriter;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-24
 * Time: 上午11:08
 * To change this template use File | Settings | File Templates.
 */
public class SolrIndexCoprocessorObserver extends BaseRegionObserver {
    private static Logger log = Logger.getLogger(SolrIndexCoprocessorObserver.class);

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String rowKey = Bytes.toString(put.getRow());
        try {
//            Cell cellId = put.get(Bytes.toBytes("data"), Bytes.toBytes("id")).get(0);
//            String id = new String(CellUtil.cloneValue(cellId));

            Cell cellInnerCode = put.get(Bytes.toBytes("data"), Bytes.toBytes("inner_code")).get(0);
            String innerCode = new String(CellUtil.cloneValue(cellInnerCode));

            Cell cellNodeId = put.get(Bytes.toBytes("data"), Bytes.toBytes("node_id")).get(0);
            String nodeId = new String(CellUtil.cloneValue(cellNodeId));

            Cell cellPayType = put.get(Bytes.toBytes("data"), Bytes.toBytes("pay_type")).get(0);
            String payType = new String(CellUtil.cloneValue(cellPayType));

            Cell cellCts = put.get(Bytes.toBytes("data"), Bytes.toBytes("cts")).get(0);
            String cts = new String(CellUtil.cloneValue(cellCts));

            Cell cellTraSeq = put.get(Bytes.toBytes("data"), Bytes.toBytes("tra_seq")).get(0);
            String traSeq = new String(CellUtil.cloneValue(cellTraSeq));

            cts=cts.replace("-","");
            cts=cts.replace(" ","");
            cts=cts.replace(":","");

            VmMoney vm = new VmMoney();
            vm.setCts(cts);
            vm.setId(new Integer(id));
            vm.setInnerCode(innerCode);
            vm.setNodeId(new Integer(nodeId));
            vm.setPayType(new Integer(payType));
            vm.setRowKey(rowKey);
            vm.setTraSeq(traSeq);

            SolrWriter so = new SolrWriter();
            so.addDocToCache(vm);
        } catch (Exception ex){
            log.info("write "+rowKey+" to solr fail:"+ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String rowKey = Bytes.toString(delete.getRow());
        try {
            SolrWriter so = new SolrWriter();
            so.deleteDoc(rowKey);
        } catch (Exception ex){
            log.info("delete "+rowKey+" from solr fail:"+ex.getMessage());
            ex.printStackTrace();
        }
    }
}
