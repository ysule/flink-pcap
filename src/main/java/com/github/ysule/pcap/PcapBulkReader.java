package com.github.ysule.pcap;


import com.github.ysule.model.PacketData;
import io.pkts.Pcap;
import io.pkts.framer.FramingException;
import io.pkts.packet.*;
import io.pkts.protocol.Protocol;
import org.apache.flink.connector.file.src.reader.BulkFormat.Reader;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.FSDataInputStream;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class PcapBulkReader implements Reader<PacketData> {
    private final Pcap pcap;
    private final int batch;
    private final FSDataInputStream stream;

    public PcapBulkReader(FSDataInputStream fstream, int batch) throws IOException {
        this.stream = fstream;
        this.pcap = Pcap.openStream(stream);
        this.batch = batch;
    }

    @Override
    @Nullable
    public RecordIterator<PacketData> readBatch() throws IOException, FramingException {
        final List<PacketData> dataBatch = new ArrayList<>();
        try {
            pcap.loop((Packet packet) -> {
                PacketData record = mapToPacketData(packet);
                dataBatch.add(record);
                return dataBatch.size() < batch;
            });
        } catch (Exception e) {
            throw new IOException("Error while parsing data:", e.getCause());
        }
        if (dataBatch.isEmpty()) {
            return null;
        }
        return new PcapRecordIterator(dataBatch);
    }

    private PacketData mapToPacketData(Packet packet) throws IOException {
        PacketData record;
        record = new PacketData();
        record.setArrivalTS(packet.getArrivalTime());

        //Layer 2: Data Link Layer value extraction
        if (packet.hasProtocol(Protocol.ETHERNET_II)) {
            MACPacket macPacket = (MACPacket) packet.getPacket(Protocol.ETHERNET_II);
            record.setSrcMac(macPacket.getSourceMacAddress());
            record.setDstMac(macPacket.getDestinationMacAddress());
        }
        //Layer 3: Network Layer value extraction
        if (packet.hasProtocol(Protocol.IPv4)) {
            IPv4Packet ip = (IPv4Packet) packet.getPacket(Protocol.IPv4);
            record.setSrcIp(ip.getSourceIP());
            record.setDstIp(ip.getDestinationIP());
            record.setIpVersion(ip.getVersion());
            record.setProtocol(ip.getProtocol().getName());
            record.setPayload(ip.getPayload().getArray());
            record.setPayloadSize(ip.getPayload().getArray().length);
        }
        if (packet.hasProtocol(Protocol.IPv6)) {
            IPv6Packet ip = (IPv6Packet) packet.getPacket(Protocol.IPv6);
            record.setSrcIp(ip.getSourceIP());
            record.setDstIp(ip.getDestinationIP());
            record.setIpVersion(ip.getVersion());
            record.setProtocol(ip.getProtocol().getName());
            record.setPayload(ip.getPayload().getArray());
            record.setPayloadSize(ip.getPayload().getArray().length);
            record.setFlowLabel(ip.getFlowLabel());
            record.setHopLimit(ip.getHopLimit());
            record.setTrafficClass(ip.getTrafficClass());
        }
        //Layer 4: Transport Layer value extraction
        if (packet.hasProtocol(Protocol.TCP)) {
            TCPPacket tcpPacket = (TCPPacket) packet.getPacket(Protocol.TCP);
            record.setSrcPort(tcpPacket.getSourcePort());
            record.setDstPort(tcpPacket.getDestinationPort());
            record.setTcpACK(tcpPacket.isACK());
            record.setTcpSYN(tcpPacket.isSYN());
            record.setTcpFIN(tcpPacket.isFIN());
            record.setTcpCWR(tcpPacket.isCWR());
            record.setTcpPSH(tcpPacket.isPSH());
            record.setTcpNS(tcpPacket.isNS());
            record.setTcpRST(tcpPacket.isRST());
            record.setTcpECE(tcpPacket.isECE());
            record.setTcpURG(tcpPacket.isURG());
        } else if (packet.hasProtocol(Protocol.UDP)) {
            UDPPacket udpPacket = (UDPPacket) packet.getPacket(Protocol.UDP);
            record.setSrcPort(udpPacket.getSourcePort());
            record.setDstPort(udpPacket.getDestinationPort());
            record.setProtocol(udpPacket.getProtocol().getName());
            record.setTcpACK(false);
            record.setTcpSYN(false);
            record.setTcpFIN(false);
            record.setTcpCWR(false);
            record.setTcpPSH(false);
            record.setTcpNS(false);
            record.setTcpRST(false);
            record.setTcpECE(false);
            record.setTcpURG(false);
        }
        return record;
    }

    /**
     * Closes the reader and should release all resources.
     */
    @Override
    public void close() throws IOException {
        stream.close();
    }

    private static class PcapRecordIterator implements RecordIterator<PacketData> {
        private final List<PacketData> records;
        private int index = 0;

        public PcapRecordIterator(List<PacketData> records) {
            this.records = records;
        }

        @Nullable
        @Override
        public RecordAndPosition<PacketData> next() {
            /*
              1.records.get(index++): reads record at current value of index(say 0) and THEN INCREMENTS the index, 0->1
              2.second constructor arg "offset", now holds the value of index incremented by 1
              3.third constructor arg "recordSkipCount", also holds the current value of index, incremented by 1.
              This is in case of a crash, Flink knows how many records to skip in a file before reading the records.
              All of this because PCAP files are non-splittable and hence have to be read entirely.
             */
            return index < records.size() ? new RecordAndPosition<>(records.get(index++), index, index) : null;
        }

        public void releaseBatch() {
            records.clear();
        }
    }
}
