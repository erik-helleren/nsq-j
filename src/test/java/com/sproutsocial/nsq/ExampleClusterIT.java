package com.sproutsocial.nsq;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ExampleClusterIT {
    private static Logger logger = LoggerFactory.getLogger(ExampleClusterIT.class);

    private NsqDockerCluster cluster;
    
    @Before
    public void setup() {
        cluster = NsqDockerCluster.builder()
            .withNsqdCount(3)
            .start()
            .awaitExposedPorts();
    }

    @After
    public void teardown() {
        cluster.shutdown();
    }

    @Test
    public void simplePublisher() throws Exception {
        assertEquals(3, cluster.getNsqdNodes().size());
        Publisher publisher = null;
        try {
            publisher = new Publisher(cluster.getNsqdNodes().get(0).getTcpHostAndPort().toString(),
                                                      cluster.getNsqdNodes().get(1).getTcpHostAndPort().toString());
            publisher.publish("test_topic", new byte[]{0x01, 0x02, 0x03, 0x04});
        } finally {
            if (publisher != null) {
                publisher.stop();
            }
        }
    }

    @Test
    public void networkDisconnects() throws Exception {
        final NsqDockerCluster.NsqdNode node = cluster.getNsqdNodes().get(0);
        logger.info("Sleeping for 30 seconds before disconnecting container: {}, on hostname: {}", node.getContainerId(), node.getTcpHostAndPort());
        Thread.sleep(30_000);
        cluster.disconnectNetworkFor(node);
        logger.info("Sleeping for 30 seconds: Network disconnected");
        Thread.sleep(30_000);
        cluster.reconnectNetworkFor(node);
        logger.info("Sleeping for 30 seconds: Network re-enabled");
        Thread.sleep(30_000);
    }
}
