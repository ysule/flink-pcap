package com.github.ysule;

import com.github.ysule.model.PacketData;
import com.github.ysule.pcap.PcapBulkFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class PcapProcessor {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //for testing only, remove in final draft
        env.setParallelism(1);

        String dataPath = "file:///D:/raw-data-folder/pcap/";

        PcapBulkFormat pcapFormat = new PcapBulkFormat(10000);

        FileSource<PacketData> source = FileSource
                .forBulkFileFormat(pcapFormat, new Path(dataPath))
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        DataStream<PacketData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "pcap");

        stream.print();

        env.execute("flink-pcap-data-ingestor");
    }
}
