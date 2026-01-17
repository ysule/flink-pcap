package com.github.ysule.pcap;

import com.github.ysule.model.PacketData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

public class PcapBulkFormat implements BulkFormat<PacketData, FileSourceSplit> {
    private final int batchSize;

    public PcapBulkFormat(int batchSize) {
        this.batchSize = batchSize;
    }


    @Override
    public Reader<PacketData> createReader(Configuration config, FileSourceSplit split) throws IOException {
        Path path = split.path();
        FileSystem fs = path.getFileSystem();
        FSDataInputStream stream = fs.open(path);
        return new PcapBulkReader(stream, batchSize);
    }

    @Override
    public Reader<PacketData> restoreReader(Configuration config, FileSourceSplit split) throws IOException {
        return createReader(config, split);
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<PacketData> getProducedType() {
        return TypeInformation.of(com.github.ysule.model.PacketData.class);
    }
}

