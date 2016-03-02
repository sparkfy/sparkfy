package com.github.sparkfy.network.util;

import com.github.sparkfy.network.TransportContext;
import com.github.sparkfy.network.server.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

/**
 * Created by huangyu on 16/3/1.
 */
public class SetupServer {

    public static TransportServer reliableServer(String hostPorts, int port, Map<String, String> conf) {
        return new TransportContext(new TransportConf("ReliableRpcServer",
                new MapConfigProvider(conf)), new ReliableRpcHandler(hostPorts, conf)).
                createServer(port, new LinkedList<TransportServerBootstrap>());
    }

    public static TransportServer partitionServer(String hostPorts, int port, Map<String, String> conf, Partitioner partitioner) {
        return new TransportContext(new TransportConf("PartitionRpcServer",
                new MapConfigProvider(conf)), new PartitionRpcHandler(hostPorts, conf, partitioner)).
                createServer(port, new LinkedList<TransportServerBootstrap>());
    }

    public static TransportServer partitionServer(String hostPorts, int port, Map<String, String> conf) {
        return new TransportContext(new TransportConf("PartitionRpcServer",
                new MapConfigProvider(conf)), new PartitionRpcHandler(hostPorts, conf, new Partitioner() {
            @Override
            public int partition(ByteBuffer byteBuffer, int length) {
                return Math.abs(Objects.hashCode(byteBuffer.hashCode())) % length;
            }
        })).
                createServer(port, new LinkedList<TransportServerBootstrap>());
    }

}
