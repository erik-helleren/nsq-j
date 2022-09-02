package com.sproutsocial.nsq;

import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Optional;

import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.okhttp.OkDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.api.DockerClient;

public class NsqDockerCluster {
    public static class Builder {
        private int nsqdCount;
        private String nsqdImage;
        private String lookupImage;
        private boolean createLookupNode;

        public Builder() {
            this.nsqdCount = 1;
            this.nsqdImage = "nsqio/nsq:v0.3.8";
            this.lookupImage = "nsqio/nsq:v0.3.8";
            this.createLookupNode = true;
        }

        public Builder withNsqdCount(final int count) {
            this.nsqdCount = count;
            return this;
        }

        public Builder withNsqdImage(final String image) {
            this.nsqdImage = image;
            return this;
        }

        public Builder withLookupImage(final String image) {
            this.lookupImage = image;
            return this;
        }

        public Builder withLookupNode(final boolean enabled) {
            this.createLookupNode = enabled;
            return this;
        }

        public NsqDockerCluster start() {
            // TODO: Fill in startup implementation
            final DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
            final DockerHttpClient dockerHttpClient = new OkDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();
            final DockerClient dockerClient = DockerClientImpl.getInstance(config, dockerHttpClient);
            dockerClient.pingCmd().exec();
            return new NsqDockerCluster(
                dockerClient,
                java.util.Collections.emptyList(),
                Optional.empty());
        }
    }

    public static class ConnectableNode {
        protected final HostAndPort hostAndPort;
        protected final String containerId;

        public ConnectableNode(final HostAndPort hostAndPort,
                               final String containerId) {
            this.hostAndPort = hostAndPort;
            this.containerId = containerId;
        }

        public final HostAndPort getHostAndPort() {
            return hostAndPort;
        }

        public final String getContainerId() {
            return containerId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("hostAndPort", hostAndPort)
                .add("containerId", containerId)
                .toString();
        }
    }

    public static class NsqdNode extends ConnectableNode {
        public NsqdNode(final HostAndPort hostAndPort,
                        final String containerId) {
            super(hostAndPort, containerId);
        }
    }

    public static class NsqLookupNode extends ConnectableNode {
        public NsqLookupNode(final HostAndPort hostAndPort,
                             final String containerId) {
            super(hostAndPort, containerId);
        }
    }

    private final DockerClient dockerClient;
    private final List<NsqdNode> nsqds;
    private final Optional<NsqLookupNode> lookup;

    public NsqDockerCluster(final DockerClient dockerClient,
                            final List<NsqdNode> nsqds,
                            final Optional<NsqLookupNode> lookup)  {
        this.dockerClient = dockerClient;
        this.nsqds = nsqds;
        this.lookup = lookup;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final List<NsqdNode> getNsqdNodes() {
        return nsqds;
    }

    public final Optional<NsqLookupNode> getLookupNode() {
        return lookup;
    }

    public void shutdown() {
        // TODO: Implement shutdown and cleanup of the cluster
    }

    public void disconnectNetworkFor(final ConnectableNode node) {
        // TODO: Cut off the host from the underlying docker network
    }

    public void reconnectNetworkFor(final ConnectableNode node) {
        // TODO: Re-connect the host from the underlying docker network
    }
}
