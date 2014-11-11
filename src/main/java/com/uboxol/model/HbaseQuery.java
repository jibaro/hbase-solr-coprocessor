package com.uboxol.model;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-30
 * Time: 上午11:41
 * To change this template use File | Settings | File Templates.
 */
public class HbaseQuery {
    int count;
    String lastKey;
    List<VmMoney> vmMoneyList;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getLastKey() {
        return lastKey;
    }

    public void setLastKey(String lastKey) {
        this.lastKey = lastKey;
    }

    public List<VmMoney> getVmMoneyList() {
        return vmMoneyList;
    }

    public void setVmMoneyList(List<VmMoney> vmMoneyList) {
        this.vmMoneyList = vmMoneyList;
    }
}
