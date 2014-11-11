package com.uboxol.solr;

import com.uboxol.hbase.VmMoneyOperater;
import com.uboxol.model.HbaseQuery;
import com.uboxol.model.VmMoney;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-29
 * Time: 下午4:43
 * To change this template use File | Settings | File Templates.
 */
public class TestVmMoneyOperater {


    @Test
    public void putOneTest(){
        VmMoneyOperater vo = new VmMoneyOperater();
        List<VmMoney> vmmList = new ArrayList<VmMoney>();
        VmMoney vm = new VmMoney("test1",0,203134,10,"0001222","2014-10-29 15:00:00");
        vmmList.add(vm);
        vo.putVmMoney(vmmList);
    }

    @Test
    public void getOnTest(){
        VmMoneyOperater vo = new VmMoneyOperater();
        vo.getVmMoneyOne("test1");
    }


    @Test
    public void getSource(){
        VmMoneyOperater vo = new VmMoneyOperater();
        HbaseQuery hq =  vo.getVmMoney("3212171","20141020000000","20141029000000",null,100);
        System.out.println(hq.getVmMoneyList().size());
    }

    @Test
    public void readWriteTest(){
        VmMoneyOperater vo = new VmMoneyOperater();
        long start = System.currentTimeMillis();
        String lastKey = "725101_79859173884240_T14090254793751000215_830651574";
        for(int i=0;i<20;i++){
            long starte = System.currentTimeMillis();
            HbaseQuery hq =  vo.getVmMoney("0","20141020000000","20141029000000",lastKey,100000);
            vo.putVmMoney(hq.getVmMoneyList());
            lastKey =hq.getLastKey();
            System.out.println("each cost:"+(System.currentTimeMillis()-starte)+"  count:"+hq.getCount());
            if (lastKey==null|| lastKey==""){
                break;
            }
        }

        System.out.println("total cost:"+(System.currentTimeMillis()-start));

    }
}
