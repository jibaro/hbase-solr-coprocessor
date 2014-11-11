package com.uboxol.solr;

import com.uboxol.model.VmMoney;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-23
 * Time: 下午5:49
 * To change this template use File | Settings | File Templates.
 */
public class SolrWriter {
    private static Logger log = Logger.getLogger(SolrWriter.class);

    public static String urlSolr = "";     //solr地址
    private static String defaultCollection = "";  //默认collection
    private static int zkClientTimeOut =0 ;//zk客户端请求超时间
    private static int zkConnectTimeOut =0;//zk客户端连接超时间
    private static CloudSolrServer solrserver = null;

    private static int maxCacheCount = 0;   //缓存大小，当达到该上限时提交
    private static Vector<VmMoney> cache = null;   //缓存
    public  static Lock commitLock =new ReentrantLock();  //在添加缓存或进行提交时加锁

    private static int maxCommitTime = 60; //最大提交时间，s

    static {
        Configuration conf = HBaseConfiguration.create();
        urlSolr = conf.get("hbase.solr.zklist", "192.168.12.1:2181,192.168.12.2:2181,192.168.12.3:2181");
        defaultCollection = conf.get("hbase.solr.collection","collection1");
        zkClientTimeOut = conf.getInt("hbase.solr.zkClientTimeOut", 10000);
        zkConnectTimeOut = conf.getInt("hbase.solr.zkConnectTimeOut", 10000);
        maxCacheCount = conf.getInt("hbase.solr.maxCacheCount", 10000);
        maxCommitTime =  conf.getInt("hbase.solr.maxCommitTime", 60*5);

        log.info("solr init param"+urlSolr+"  "+defaultCollection+"  "+zkClientTimeOut+"  "+zkConnectTimeOut+"  "+maxCacheCount+"  "+maxCommitTime);
        try {
            cache=new Vector<VmMoney>(maxCacheCount);

            solrserver = new CloudSolrServer(urlSolr);
            solrserver.setDefaultCollection(defaultCollection);
            solrserver.setZkClientTimeout(zkClientTimeOut);
            solrserver.setZkConnectTimeout(zkConnectTimeOut);

            //启动定时任务，第一次延迟10执行,之后每隔指定时间执行一次
            Timer timer=new Timer();
            timer.schedule(new CommitTimer(),10*1000,maxCommitTime*1000);
        } catch (Exception ex){
            ex.printStackTrace();
        }

    }

    public void inputDoc(List<VmMoney> vmMoneyList) throws IOException, SolrServerException {
        if (vmMoneyList == null || vmMoneyList.size() == 0) {
            return;
        }
        List<SolrInputDocument> doclist= new ArrayList<SolrInputDocument>(vmMoneyList.size());
        for (VmMoney vm : vmMoneyList) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", vm.getId());
            doc.addField("node_id", vm.getNodeId());
            doc.addField("inner_code", vm.getInnerCode());
            doc.addField("pay_type", vm.getPayType());
            doc.addField("rowkey", vm.getRowKey());
            doc.addField("cts", vm.getCts());
            doc.addField("tra_seq", vm.getTraSeq());

            doclist.add(doc);
        }
        solrserver.add(doclist);
    }

    public void inputDoc(VmMoney vmMoney) throws IOException, SolrServerException {
        if (vmMoney == null) {
            return;
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", vmMoney.getId());
        doc.addField("node_id", vmMoney.getNodeId());
        doc.addField("inner_code", vmMoney.getInnerCode());
        doc.addField("pay_type", vmMoney.getPayType());
        doc.addField("rowkey", vmMoney.getRowKey());
        doc.addField("cts", vmMoney.getCts());
        doc.addField("tra_seq", vmMoney.getTraSeq());

        solrserver.add(doc);

    }

    public void deleteDoc(List<String> rowkeys) throws IOException, SolrServerException {
        if (rowkeys == null || rowkeys.size() == 0) {
            return;
        }
        solrserver.deleteById(rowkeys);
    }

    public void deleteDoc(String rowkey) throws IOException, SolrServerException {

        solrserver.deleteById(rowkey);
    }

    /**
     * 添加记录到cache，如果cache达到maxCacheCount，则提交
     */
    public static void addDocToCache(VmMoney vmMoney) {
        commitLock.lock();
        try {
            cache.add(vmMoney);
            log.info("cache commit maxCacheCount:"+maxCacheCount);
            if (cache.size() >= maxCacheCount) {
                log.info("cache commit count:"+cache.size());
                new SolrWriter().inputDoc(cache);
                cache.clear();
            }
        } catch (Exception ex) {
            log.info(ex.getMessage());
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * 提交定时器
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                if (cache.size() > 0) { //达到容量则提交
                    log.info("timer commit count:"+cache.size());
                    new SolrWriter().inputDoc(cache);
                    cache.clear();
                }
            } catch (Exception ex) {
                log.info(ex.getMessage());
            } finally {
                commitLock.unlock();
            }
        }
    }
}
