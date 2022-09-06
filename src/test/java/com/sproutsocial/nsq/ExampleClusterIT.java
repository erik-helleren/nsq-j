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
            .start();
    }

    @After
    public void teardown() {
        cluster.shutdown();
    }

    @Test
    public void disconnectConnectNetworkExample() throws Exception {
        for (final NsqDockerCluster.NsqdNode nsqd : cluster.getNsqdNodes()) {
            logger.info("The nsqd host and port is: {}", nsqd.getHostAndPort());
        }
        final NsqDockerCluster.NsqLookupNode lookup = cluster.getLookupNode();
        logger.info("The lookup is at host and port: {}", lookup.getHostAndPort());

        if (cluster.getNsqdNodes().size() > 0) {
            final NsqDockerCluster.NsqdNode firstNode = cluster.getNsqdNodes().get(0);
            logger.info("Disconnecting the network for node: {}", firstNode);
            cluster.disconnectNetworkFor(firstNode);
            logger.info("Disconnected the network for node: {}", firstNode);

            logger.info("Re-enabling the network for node: {}", firstNode);
            cluster.reconnectNetworkFor(firstNode);
            logger.info("Re-enabled the network for node: {}", firstNode);
        }
        logger.info("Sleeping for 10 seconds before shutting down cluster");
        Thread.sleep(10_000);
    }
}
