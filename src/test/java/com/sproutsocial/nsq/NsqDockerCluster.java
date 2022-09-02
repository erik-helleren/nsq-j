package com.sproutsocial.nsq;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.okhttp.OkDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;

public class NsqDockerCluster {
    public static class Builder {
        private final ExecutorService executor = Executors.newFixedThreadPool(4);
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
            final CreatedContainers containers = createAndStartContainers(dockerClient);
            return new NsqDockerCluster(
                dockerClient,
                containers.nsqdNodes,
                containers.lookupNode);
        }

        private CreatedContainers createAndStartContainers(final DockerClient dockerClient) {
            final List<Future<String>> nsqdContainerIds = createContainers(dockerClient, nsqdImage, nsqdCount);

            Optional<NsqLookupNode> lookupNode = Optional.empty();
            final ImmutableList.Builder<NsqdNode> nsqdNodes = new ImmutableList.Builder<>();

            try {
                if (createLookupNode) {
                    final List<Future<String>> lookupContainerId = createContainers(dockerClient, lookupImage, 1);
                    // TODO: Fix hardcoded hostname and port, after we setup port forwarding
                    lookupNode = Optional.of(new NsqLookupNode(lookupContainerId.get(0).get(), HostAndPort.fromParts("127.0.0.1", 4151)));
                }

                for (final Future<String> containerId : nsqdContainerIds) {
                    // TODO: Fix hardcoded hostname and port, after we setup port forwarding
                    nsqdNodes.add(new NsqdNode(containerId.get(), HostAndPort.fromParts("127.0.0.1", 4151)));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during container creation and start");
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            return new CreatedContainers(nsqdNodes.build(), lookupNode);
        }

        private final List<Future<String>> createContainers(final DockerClient dockerClient,
                                                            final String image,
                                                            final int count) {
            final ImmutableList.Builder<Future<String>> containerIds = new ImmutableList.Builder<>();
            for (int i = 0; i < count; i++) {
                containerIds.add(executor.submit(() -> {
                            final CreateContainerResponse response = dockerClient.createContainerCmd(image).exec();
                            return response.getId();
                        }));
            }
            return containerIds.build();
        }
    }

    private static class CreatedContainers {
        public List<NsqdNode> nsqdNodes;
        public Optional<NsqLookupNode> lookupNode;

        public CreatedContainers(final List<NsqdNode> nsqdNodes,
                                 final Optional<NsqLookupNode> lookupNode) {
            this.nsqdNodes = nsqdNodes;
            this.lookupNode = lookupNode;
        }
    }

    public static class ConnectableNode {
        protected final String containerId;
        protected final HostAndPort hostAndPort;

        public ConnectableNode(final String containerId,
                               final HostAndPort hostAndPort) {
            this.containerId = containerId;
            this.hostAndPort = hostAndPort;
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
        public NsqdNode(final String containerId,
                        final HostAndPort hostAndPort) {
            super(containerId, hostAndPort);
        }
    }

    public static class NsqLookupNode extends ConnectableNode {
        public NsqLookupNode(final String containerId,
                             final HostAndPort hostAndPort) {
            super(containerId, hostAndPort);
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
