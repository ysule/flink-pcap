package com.github.ysule.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringJoiner;

public class PacketData implements Serializable {
    public long arrivalTS;
    public String srcMac;
    public String dstMac;
    public String srcIp;
    public String dstIp;
    public int srcPort;
    public int dstPort;
    public String protocol;
    public int ipVersion;
    public int payloadSize;
    public byte[] payload;
    // TCP specific flags
    public boolean tcpSYN;
    public boolean tcpFIN;
    public boolean tcpACK;
    public boolean tcpCWR;
    public boolean tcpECE;
    public boolean tcpNS;
    public boolean tcpPSH;
    public boolean tcpRST;
    public boolean tcpURG;
    // IPV6 specific values
    public short trafficClass;
    public int flowLabel;
    public int hopLimit;


    // constructor for Flink serialization
    public PacketData() {
    }


    public PacketData(long arrivalTS, String srcMac, String dstMac, String srcIp, String dstIp, int srcPort,
                      int dstPort, String protocol, int ipVersion, int payloadSize, byte[] payload,
                      boolean tcpSYN, boolean tcpFIN, boolean tcpACK, boolean tcpCWR, boolean tcpECE,
                      boolean tcpNS, boolean tcpPSH, boolean tcpRST, boolean tcpURG, short trafficClass,
                      int flowLabel, int hopLimit) {
        this.arrivalTS = arrivalTS;
        this.srcMac = srcMac;
        this.dstMac = dstMac;
        this.srcIp = srcIp;
        this.dstIp = dstIp;
        this.srcPort = srcPort;
        this.dstPort = dstPort;
        this.protocol = protocol;
        this.ipVersion = ipVersion;
        this.payloadSize = payloadSize;
        this.payload = payload;
        this.tcpSYN = tcpSYN;
        this.tcpFIN = tcpFIN;
        this.tcpACK = tcpACK;
        this.tcpCWR = tcpCWR;
        this.tcpECE = tcpECE;
        this.tcpNS = tcpNS;
        this.tcpPSH = tcpPSH;
        this.tcpRST = tcpRST;
        this.tcpURG = tcpURG;
        this.trafficClass = trafficClass;
        this.flowLabel = flowLabel;
        this.hopLimit = hopLimit;
    }

    public long getArrivalTS() {
        return arrivalTS;
    }

    public void setArrivalTS(long arrivalTS) {
        this.arrivalTS = arrivalTS;
    }

    public String getSrcMac() {
        return srcMac;
    }

    public void setSrcMac(String srcMac) {
        this.srcMac = srcMac;
    }

    public String getDstMac() {
        return dstMac;
    }

    public void setDstMac(String dstMac) {
        this.dstMac = dstMac;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public String getDstIp() {
        return dstIp;
    }

    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(int srcPort) {
        this.srcPort = srcPort;
    }

    public int getDstPort() {
        return dstPort;
    }

    public void setDstPort(int dstPort) {
        this.dstPort = dstPort;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getIpVersion() {
        return ipVersion;
    }

    public void setIpVersion(int ipVersion) {
        this.ipVersion = ipVersion;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public boolean isTcpSYN() {
        return tcpSYN;
    }

    public void setTcpSYN(boolean tcpSYN) {
        this.tcpSYN = tcpSYN;
    }

    public boolean isTcpFIN() {
        return tcpFIN;
    }

    public void setTcpFIN(boolean tcpFIN) {
        this.tcpFIN = tcpFIN;
    }

    public boolean isTcpACK() {
        return tcpACK;
    }

    public void setTcpACK(boolean tcpACK) {
        this.tcpACK = tcpACK;
    }

    public boolean isTcpCWR() {
        return tcpCWR;
    }

    public void setTcpCWR(boolean tcpCWR) {
        this.tcpCWR = tcpCWR;
    }

    public boolean isTcpECE() {
        return tcpECE;
    }

    public void setTcpECE(boolean tcpECE) {
        this.tcpECE = tcpECE;
    }

    public boolean isTcpNS() {
        return tcpNS;
    }

    public void setTcpNS(boolean tcpNS) {
        this.tcpNS = tcpNS;
    }

    public boolean isTcpPSH() {
        return tcpPSH;
    }

    public void setTcpPSH(boolean tcpPSH) {
        this.tcpPSH = tcpPSH;
    }

    public boolean isTcpRST() {
        return tcpRST;
    }

    public void setTcpRST(boolean tcpRST) {
        this.tcpRST = tcpRST;
    }

    public boolean isTcpURG() {
        return tcpURG;
    }

    public void setTcpURG(boolean tcpURG) {
        this.tcpURG = tcpURG;
    }

    public short getTrafficClass() {
        return trafficClass;
    }

    public void setTrafficClass(short trafficClass) {
        this.trafficClass = trafficClass;
    }

    public int getFlowLabel() {
        return flowLabel;
    }

    public void setFlowLabel(int flowLabel) {
        this.flowLabel = flowLabel;
    }

    public int getHopLimit() {
        return hopLimit;
    }

    public void setHopLimit(int hopLimit) {
        this.hopLimit = hopLimit;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PacketData.class.getSimpleName() + "[", "]")
                .add("arrivalTS=" + arrivalTS)
                .add("srcMac='" + srcMac + "'")
                .add("dstMac='" + dstMac + "'")
                .add("srcIp='" + srcIp + "'")
                .add("dstIp='" + dstIp + "'")
                .add("srcPort=" + srcPort)
                .add("dstPort=" + dstPort)
                .add("protocol='" + protocol + "'")
                .add("ipVersion=" + ipVersion)
                .add("payloadSize=" + payloadSize)
                .add("payload=" + Arrays.toString(payload))
                .add("tcpSYN=" + tcpSYN)
                .add("tcpFIN=" + tcpFIN)
                .add("tcpACK=" + tcpACK)
                .add("tcpCWR=" + tcpCWR)
                .add("tcpECE=" + tcpECE)
                .add("tcpNS=" + tcpNS)
                .add("tcpPSH=" + tcpPSH)
                .add("tcpRST=" + tcpRST)
                .add("tcpURG=" + tcpURG)
                .add("trafficClass=" + trafficClass)
                .add("flowLabel=" + flowLabel)
                .add("hopLimit=" + hopLimit)
                .toString();
    }
}