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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by huangyu on 16/2/16.
 */
public class ReliableRpcHandler extends RpcHandler {

    private final Logger logger = LoggerFactory.getLogger(ReliableRpcHandler.class);

    private final StreamManager streamManager;
    private final HostPort[] hostPorts;
    private final HostPort[] refused;
    private final Object[] locks;
    private final long delay;
    private final TransportClientFactory clientFactory;
    private final ScheduledExecutorService recoverTask = ThreadUtils.newDaemonSingleThreadScheduledExecutor("recoverTask");

    public ReliableRpcHandler(final String hostPortsStr, Map<String, String> conf) {
        this.streamManager = new OneForOneStreamManager();
        this.hostPorts = HostPort.parse(hostPortsStr);
        if (this.hostPorts.length <= 0) {
            throw new IllegalArgumentException("No host:port");
        }
        locks = new Object[this.hostPorts.length];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        refused = new HostPort[this.hostPorts.length];

        TransportContext context = new TransportContext(new TransportConf("ReliableRpcClient",
                new MapConfigProvider(conf)), new NoOpRpcHandler(), true);
        clientFactory = context.createClientFactory();


        int activeClient = 0;
        for (int i = 0; i < this.hostPorts.length; i++) {
            HostPort hostPort = get(i);
            if (hostPort == null) continue;
            try {
                clientFactory.createClient(hostPort.host, hostPort.port);
                activeClient++;
            } catch (IOException e) {
                remove(i);
                logger.warn("Failed to connect client:" + hostPort.toString(), e);
            }
        }
        if (activeClient == 0) {
            throw new RuntimeException("Failed to connect all client(" + Arrays.toString(this.hostPorts) + ")");
        }

        String delayStr = conf.get("reliable.server.delay");
        delay = delayStr == null ? 1000L : Long.parseLong(delayStr);
        recoverTask.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < hostPorts.length; i++) {
                    HostPort hostPort = getRefused(i);
                    if (hostPort == null) continue;
                    try {
                        clientFactory.createClient(hostPort.host, hostPort.port);
                        recovery(i);
                    } catch (IOException e) {
                        logger.warn("Failed to connect client:" + hostPort.toString(), e);
                    }
                }
            }
        }, 0L, delay, TimeUnit.MILLISECONDS);
    }

    private HostPort get(int i) {
        synchronized (locks[i]) {
            return hostPorts[i];
        }
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


    @Override
    public void receive(TransportClient client, ByteBuffer msg, final RpcResponseCallback callback) {
        final boolean[] isResponse = new boolean[]{false};
        int activeClient = 0;
        for (int i = 0; i < hostPorts.length; i++) {
            HostPort hostPort = get(i);
            if (hostPort == null) continue;
            try {
                clientFactory.createClient(hostPort.host, hostPort.port).sendRpc(msg, new RpcResponseCallback() {
                    @Override
                    public void onSuccess(ByteBuffer response) {
                        if (!isResponse[0]) {
                            synchronized (isResponse) {
                                if (!isResponse[0]) {
                                    isResponse[0] = true;
                                    callback.onSuccess(response);
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        if (!isResponse[0]) {
                            synchronized (isResponse) {
                                if (!isResponse[0]) {
                                    isResponse[0] = true;
                                    callback.onFailure(e);
                                }
                            }
                        }
                    }
                });
                activeClient++;
            } catch (IOException e) {
                remove(i);
                logger.warn("Failed to connect client:" + hostPort.toString(), e);
            }
        }

        if (activeClient == 0) {
            throw new RuntimeException("Failed to connect all client(" + Arrays.toString(hostPorts) + ")");
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
}
