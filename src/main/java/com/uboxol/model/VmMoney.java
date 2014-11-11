package com.uboxol.model;

/**
 * Created with IntelliJ IDEA.
 * User: guojing
 * Date: 14-10-23
 * Time: 下午5:52
 * To change this template use File | Settings | File Templates.
 */
public class VmMoney {
    String rowKey;
    int id;
    int nodeId;
    int payType;
    String innerCode;
    String cts;
    String traSeq;

    public VmMoney() {
    }

    public VmMoney(String rowKey, int id, int nodeId, int payType, String innerCode, String cts) {
        this.rowKey = rowKey;
        this.id = id;
        this.nodeId = nodeId;
        this.payType = payType;
        this.innerCode = innerCode;
        this.cts = cts;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getPayType() {
        return payType;
    }

    public void setPayType(int payType) {
        this.payType = payType;
    }

    public String getInnerCode() {
        return innerCode;
    }

    public void setInnerCode(String innerCode) {
        this.innerCode = innerCode;
    }

    public String getCts() {
        return cts;
    }

    public void setCts(String cts) {
        this.cts = cts;
    }

    public String getTraSeq() {
        return traSeq;
    }

    public void setTraSeq(String traSeq) {
        this.traSeq = traSeq;
    }

    @Override
    public String toString() {
        return "VmMoney{" +
                "rowKey='" + rowKey + '\'' +
                ", id=" + id +
                ", nodeId=" + nodeId +
                ", payType=" + payType +
                ", innerCode='" + innerCode + '\'' +
                ", cts='" + cts + '\'' +
                ", traSeq='" + traSeq + '\'' +
                '}';
    }
}
