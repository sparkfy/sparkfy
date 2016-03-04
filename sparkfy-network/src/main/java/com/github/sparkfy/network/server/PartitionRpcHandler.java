package com.github.sparkfy.network.server;

import com.github.sparkfy.network.TransportContext;
import com.github.sparkfy.network.client.RpcResponseCallback;
import com.github.sparkfy.network.client.TransportClient;
import com.github.sparkfy.network.client.TransportClientFactory;
import com.github.sparkfy.network.util.MapConfigProvider;
import com.github.sparkfy.network.util.TransportConf;
import com.github.sparkfy.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.control.*;

import java.io.IOException;
import java.lang.Exception;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by huangyu on 16/2/16.
 */
public class PartitionRpcHandler extends RpcHandler {

    private final Logger logger = LoggerFactory.getLogger(PartitionRpcHandler.class);

    private final StreamManager streamManager;
    private final HostPort[] hostPorts;
    private final HostPort[] refused;
    private final Object[] locks;
    private final Object recoveryLock = new Object();
    private final Object connectLock = new Object();
    private final long delay;
    private final long timeoutMs;
    private final Partitioner partitioner;
    private final TransportClientFactory clientFactory;
    private ScheduledExecutorService recoverTask = null;
    private ScheduledExecutorService connectTask = null;

    public PartitionRpcHandler(final String hostPortsStr, Map<String, String> conf, Partitioner partitioner) {

        if (partitioner == null) {
            throw new IllegalArgumentException("Partitioner is null");
        }
        this.partitioner = partitioner;
        this.hostPorts = HostPort.parse(hostPortsStr);
        if (this.hostPorts.length <= 0) {
            throw new IllegalArgumentException("No host:port");
        }
        this.streamManager = new OneForOneStreamManager();
        locks = new Object[this.hostPorts.length];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        refused = new HostPort[this.hostPorts.length];

        TransportContext context = new TransportContext(new TransportConf("PartitionRpcClient",
                new MapConfigProvider(conf)), new NoOpRpcHandler(), true);
        clientFactory = context.createClientFactory();


//        int activeClientNum = 0;
        for (int i = 0; i < this.hostPorts.length; i++) {
            HostPort hostPort = get(i);
            if (hostPort == null) continue;
            try {
                clientFactory.createClient(hostPort.host, hostPort.port);
//                activeClientNum++;
            } catch (IOException e) {
                remove(i);
                logger.warn("Failed to connect client:" + hostPort.toString(), e);
            }
        }
//        if (activeClientNum == 0) {
//            throw new RuntimeException("Failed to connect all client(" + Arrays.toString(this.hostPorts) + ")");
//        }

        String delayStr = conf.get("reliable.server.delay");
        String timeoutMsStr = conf.get("reliable.server.timeoutMs");
        delay = delayStr == null ? 1000L : Long.parseLong(delayStr);
        timeoutMs = timeoutMsStr == null ? 1000L : Long.parseLong(timeoutMsStr);

    }

    private HostPort get(int i) {
        synchronized (locks[i]) {
            return hostPorts[i];
        }
    }

    private int getPartition(ByteBuffer msg) {
        int partition = partitioner.partition(msg.duplicate(), hostPorts.length);
        if (get(partition) != null) {
            return partition;
        }
        for (int i = partition + 1; i < hostPorts.length; i++) {
            if (get(i) != null) {
                return i;
            }
        }
        for (int i = partition - 1; i >= 0; i--) {
            if (get(i) != null) {
                return i;
            }
        }
        return -1;
    }

    private HostPort getRefused(int i) {
        synchronized (locks[i]) {
            return refused[i];
        }
    }

    private void remove(int i) {
        synchronized (locks[i]) {
            if (refused[i] == null && hostPorts[i] != null) {
                refused[i] = hostPorts[i];
                hostPorts[i] = null;
            }
        }
    }

    private void recovery(int i) {
        synchronized (locks[i]) {
            if (refused[i] != null && hostPorts[i] == null) {
                hostPorts[i] = refused[i];
                refused[i] = null;
            }
        }
    }

    private void startRecoveryTask(final ByteBuffer msg) {
        if (recoverTask == null) {
            synchronized (recoveryLock) {
                if (recoverTask == null) {
                    recoverTask = ThreadUtils.newDaemonSingleThreadScheduledExecutor("recoveryTask");
                    recoverTask.scheduleWithFixedDelay(new RecoveryTask(msg), 0L, delay, TimeUnit.MILLISECONDS);
                }
            }
        }

    }


    private void startConnectTask(final ByteBuffer msg) {
        if (connectTask == null) {
            synchronized (connectLock) {
                if (connectTask == null) {
                    connectTask = ThreadUtils.newDaemonSingleThreadScheduledExecutor("connectTask");
                    connectTask.scheduleWithFixedDelay(new ConnectTask(msg), 0L, delay, TimeUnit.MILLISECONDS);
                }
            }
        }

    }


    @Override
    public void receive(TransportClient client, ByteBuffer msg, final RpcResponseCallback callback) {
        startConnectTask(msg);
        startRecoveryTask(msg);
        int partition = getPartition(msg);
        if (partition == -1) {
            throw new RuntimeException("Failed to connect all client(" + Arrays.toString(hostPorts) + ")");
        }
        HostPort hostPort = get(partition);
        if (hostPort == null) {
            throw new RuntimeException("Partitioned but failed to connect client:" + hostPort.toString());
        }
        try {
            clientFactory.createClient(hostPort.host, hostPort.port).sendRpc(msg, new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                    copy.put(response);
                    // flip "copy" to make it readable
                    copy.flip();
                    callback.onSuccess(copy);
                }

                @Override
                public void onFailure(Throwable e) {
                    callback.onFailure(e);
                }
            });
        } catch (IOException e) {
            remove(partition);
            throw new RuntimeException("Partitioned but failed to connect client:" + hostPort.toString(), e);
        }
    }


    @Override
    public StreamManager getStreamManager() {
        //TODO:transport to remote host:port
        return streamManager;
    }


    static class HostPort {

        public final String host;
        public final int port;

        public static HostPort[] parse(String hostPorts) {
            String[] sps = hostPorts.split(",");
            HostPort[] result = new HostPort[sps.length];
            for (int i = 0; i < result.length; i++) {
                String[] sp = sps[i].split(":");
                result[i] = new HostPort(sp[0], Integer.parseInt(sp[1]));
            }
            return result;
        }

        public HostPort(String host, int port) {
            if (port < 0) throw new IllegalArgumentException("Illegal host:pot(" + host + ":" + port + ")");
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    class ConnectTask implements Runnable {
        private final ByteBuffer msg;

        public ConnectTask(ByteBuffer msg) {
//            ByteBuffer dmsg = msg.duplicate();
//            byte[] bytes = new byte[dmsg.remaining()];
//            dmsg.get(bytes);
//            this.msg = ByteBuffer.wrap(bytes);
            this.msg = msg.duplicate();
        }

        @Override
        public void run() {
            for (int i = 0; i < hostPorts.length; i++) {
                HostPort hostPort = get(i);
                if (hostPort == null) continue;
                TransportClient client = null;
                try {
                    client = clientFactory.createClient(hostPort.host, hostPort.port);
//                    synchronized (client) {
//                        client.sendRpcSync(msg.duplicate(), timeoutMs);
//                    }
                } catch (Exception e) {
//                    client.close();
                    remove(i);
                    logger.warn("Failed to connect client:" + hostPort.toString(), e);
                }
            }
        }
    }

    class RecoveryTask implements Runnable {
        private final ByteBuffer msg;

        public RecoveryTask(ByteBuffer msg) {
//            ByteBuffer dmsg = msg.duplicate();
//            byte[] bytes = new byte[dmsg.remaining()];
//            dmsg.get(bytes);
//            this.msg = ByteBuffer.wrap(bytes);
            this.msg = msg.duplicate();
        }

        @Override
        public void run() {
            for (int i = 0; i < hostPorts.length; i++) {
                HostPort hostPort = getRefused(i);
                if (hostPort == null) continue;
                TransportClient client = null;
                try {
                    client = clientFactory.createClient(hostPort.host, hostPort.port);
//                    synchronized (client) {
//                        client.sendRpcSync(msg.duplicate(), timeoutMs);
//                    }
                    recovery(i);
                } catch (Exception e) {
//                    if (client != null) {
//                        client.close();
//                    }
                    logger.warn("Failed to connect client:" + hostPort.toString(), e);
                }
            }
        }
    }


}
