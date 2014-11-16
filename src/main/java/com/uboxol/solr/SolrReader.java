package com.uboxol.solr;

import com.uboxol.model.VmMoney;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-31
 * Time: 下午1:55
 * To change this template use File | Settings | File Templates.
 */
public class SolrReader {
    private static final String urlSolr="http://192.168.8.92:8983/solr";

    public List<VmMoney> GetDoc() {
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();

        try {
            SolrServer solrserver = new HttpSolrServer(urlSolr);
            SolrQuery sq = new SolrQuery();

            sq.set("q", "inner_code:1001012");
            sq.setSort("rowkey", SolrQuery.ORDER.asc);
            sq.setStart(0);
            sq.setRows(10);
            QueryResponse qr = solrserver.query(sq);
            SolrDocumentList sdList = qr.getResults();
            System.out.println(sdList.getNumFound() + "  " + sdList.size());
            for (SolrDocument sd : sdList) {
                VmMoney vm = new VmMoney();
                vm.setRowKey(sd.getFieldValue("rowkey").toString());
                vmMoneyList.add(vm);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return vmMoneyList;
    }

    /**
     * 获取列表
     * @param vmCodes
     * @param from
     * @param to
     * @param ptype
     * @param page
     * @param count
     * @return
     */
    public List<VmMoney> GetDocsByVms(String[] vmCodes,String from,String to,String ptype,int page,int count){
        if (vmCodes==null || vmCodes.length==0){
            return null;
        }
        List<VmMoney> vmMoneyList = new ArrayList<VmMoney>();

        try {
            SolrServer solrserver = new HttpSolrServer(urlSolr);
            SolrQuery sq = new SolrQuery();

            StringBuffer sb = new StringBuffer();
            //时间
            sb.append("cts:["+from+" TO "+to+"] ");

            //机器号
            StringBuffer qVmCode = new StringBuffer("(");
            for (int i=0;i<vmCodes.length;i++){
                if (i==vmCodes.length-1){
                    qVmCode.append("inner_code:"+vmCodes[i]);
                }else{
                    qVmCode.append("inner_code:"+vmCodes[i]+" OR ");
                }
            }
            qVmCode.append(")");
            sb.append(" AND ");
            sb.append(qVmCode);

            //支付类型
            StringBuffer ptypeSb = new StringBuffer("(");
            if (ptype!=null && ptype!=""){
                String[] ptypes =  ptype.split(",");
                for (int i=0;i<ptypes.length;i++){
                    if (i==vmCodes.length-1){
                        ptypeSb.append("pay_type:"+ptypes[i]);
                    }else{
                        ptypeSb.append("pay_type:"+ptypes[i]+" OR ");
                    }
                }
                ptypeSb.append(")");

                sb.append(" AND ");
                sb.append(ptypeSb);
            }


            System.out.println(sb.toString());

            sq.set("q", sb.toString());
            sq.setSort("cts", SolrQuery.ORDER.asc);

            int start = page*count;
            sq.setStart(start);
            sq.setRows(count);
            QueryResponse qr = solrserver.query(sq);
            SolrDocumentList sdList = qr.getResults();
            System.out.println(sdList.getNumFound() + "  " + sdList.size());
            for (SolrDocument sd : sdList) {
                VmMoney vm = new VmMoney();
                vm.setRowKey(sd.getFieldValue("rowkey").toString());
                vmMoneyList.add(vm);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return vmMoneyList;
    }
}
