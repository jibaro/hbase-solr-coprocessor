package com.uboxol.solr;


import com.uboxol.hbase.VmMoneyOperater;
import com.uboxol.model.VmMoney;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-23
 * Time: 下午5:50
 * To change this template use File | Settings | File Templates.
 */
public class TestSolrOperater {

    @Test
    public void InputTest() throws InterruptedException, IOException, SolrServerException {
        VmMoney v1 =new VmMoney("test17",1010,4019,10,"0001421","20141023120000");
        VmMoney v2 =new VmMoney("test18",1011,4020,10,"0001422","20141024120000");
        VmMoney v3 =new VmMoney("test19",1012,4021,21,"0001423","20141025120000");
//        VmMoney v4 =new VmMoney("test13",1013,4022,30,"0001424","20141026120000");

        List<VmMoney> list =new ArrayList<VmMoney>();
        list.add(v1);
        list.add(v2);
        list.add(v3);
//        list.add(v4);

        SolrWriter so = new SolrWriter();
        so.inputDoc(list);
        Thread.sleep(100000);
    }

    @Test
    public void GetTest(){
        SolrReader so = new SolrReader();
        List<VmMoney> list = so.GetDoc();
        System.out.println(new Date());
        for (VmMoney vm :list){
//           System.out.println(vm.toString());
        }
    }

    @Test
    public  void FacetTest(){
        try {
            String urlSolr="http://127.0.0.1:8983/solr";
            SolrServer solrserver = new HttpSolrServer(urlSolr);
            SolrQuery sq = new SolrQuery();

            sq.set("q","*:*");
            sq.setFacet(true).addFacetField("pay_type").addFacetField("cts");
            QueryResponse qr = solrserver.query(sq);
            List<FacetField> ffs = qr.getFacetFields();
            for (FacetField ff : ffs){
                System.out.println(ff.getName()+" "+ff.getValueCount()+" "+ff.getValues().toString());
                List<FacetField.Count> cl =  ff.getValues();
                System.out.println(ff.getGap());
                for (FacetField.Count c:cl){
                    System.out.println(c.getName()+" "+c.getCount());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void GetByVmCode(){
        SolrReader so = new SolrReader();
        long start = System.currentTimeMillis();
        List<VmMoney> list = so.GetDocsByVms(new String[]{"1021072"},"20141015000000","20141020000000","",0,20);

        List<String> keyList = new ArrayList<String>(list.size());
        for (VmMoney vm :list){
            keyList.add(vm.getRowKey());
        }

        VmMoneyOperater vo = new VmMoneyOperater();
        List<VmMoney> rlist = vo.getVmMoneyByRowKey(keyList);
        System.out.println(rlist);
        for (VmMoney vm :rlist){
            System.out.println(vm.toString());
        }
        System.out.println("total cost:"+(System.currentTimeMillis()-start));
    }
}
