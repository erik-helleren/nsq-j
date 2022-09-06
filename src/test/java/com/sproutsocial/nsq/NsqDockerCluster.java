package com.sproutsocial.nsq;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.okhttp.OkDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;

public class NsqDockerCluster {
    public static class ContainerConfig {
        public String image;
        public String nameFormat;
        public String entryPointFormat;

        public ContainerConfig(final String image,
                               final String nameFormat,
                               final String entryPointFormat) {
            this.image = image;
            this.nameFormat = nameFormat;
            this.entryPointFormat = entryPointFormat;
        }
    }

    public static class CreatedContainer {
        public String containerId;
        public String name;

        public CreatedContainer(final String containerId,
                                final String name) {
            this.containerId = containerId;
            this.name = name;
        }
    }

    private static class CreatedContainers {
        public List<NsqdNode> nsqdNodes;
        public NsqLookupNode lookupNode;

        public CreatedContainers(final List<NsqdNode> nsqdNodes,
                                 final NsqLookupNode lookupNode) {
            this.nsqdNodes = nsqdNodes;
            this.lookupNode = lookupNode;
        }
    }

    private static final ContainerConfig DEFAULT_NSQD_CONFIG = new ContainerConfig(
        "nsqio/nsq:v0.3.8",
        "nsqd-cluster-%d",
        "/nsqd --lookupd-tcp-address=%s --broadcast-address=%s");

    private static final ContainerConfig DEFAULT_LOOKUP_CONFIG = new ContainerConfig(
        "nsqio/nsq:v0.3.8",
        "nsq-lookup-cluster-%d",
        "/nsqlookupd");

    public static class Builder {
        private final ExecutorService executor = Executors.newFixedThreadPool(4);
        private int nsqdCount;
        private ContainerConfig nsqdConfig;
        private ContainerConfig lookupConfig;

        public Builder() {
            this.nsqdCount = 1;
            this.nsqdConfig = DEFAULT_NSQD_CONFIG;
            this.lookupConfig = DEFAULT_LOOKUP_CONFIG;
        }

        public Builder withNsqdCount(final int count) {
            this.nsqdCount = count;
            return this;
        }

        public Builder withNsqdConfig(final ContainerConfig config) {
            this.nsqdConfig = config;
            return this;
        }

        public Builder withLookupImage(final ContainerConfig config) {
            this.lookupConfig = config;
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
            final List<Future<CreatedContainer>> lookupContainers = createContainers(
                dockerClient, lookupConfig, 1, ImmutableList.of());
            final ImmutableList.Builder<NsqdNode> nsqdNodes = new ImmutableList.Builder<>();
            final NsqLookupNode lookupNode;
            try {
                final List<Future<CreatedContainer>> nsqdContainers = createContainers(
                    dockerClient, nsqdConfig, nsqdCount, ImmutableList.of(lookupContainers.get(0).get().name, "$${containerName}"));

                lookupNode = new NsqLookupNode(lookupContainers.get(0).get().containerId, HostAndPort.fromParts("127.0.0.1", 4151));

                for (final Future<CreatedContainer> createdNsqd : nsqdContainers) {
                    // TODO: Fix hardcoded hostname and port, after we setup port forwarding
                    nsqdNodes.add(new NsqdNode(createdNsqd.get().containerId, HostAndPort.fromParts("127.0.0.1", 4151)));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during container creation and start");
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            return new CreatedContainers(nsqdNodes.build(), lookupNode);
        }

        private final List<Future<CreatedContainer>> createContainers(final DockerClient dockerClient,
                                                                      final ContainerConfig config,
                                                                      final int count,
                                                                      final List<String> entryPointBinds) {
            final ImmutableList.Builder<Future<CreatedContainer>> createdContainers = new ImmutableList.Builder<>();
            for (int i = 0; i < count; i++) {
                final String containerName = String.format(config.nameFormat, i);
                final String entryPoint = buildEntryPoint(containerName, config.entryPointFormat, entryPointBinds);
                createdContainers.add(executor.submit(() -> {
                            final CreateContainerResponse response = dockerClient.createContainerCmd(config.image)
                                .withName(containerName)
                                .withEntrypoint(entryPoint)
                                .exec();
                            return new CreatedContainer(response.getId(), containerName);
                        }));
            }
            return createdContainers.build();
        }

        private final String buildEntryPoint(final String containerName,
                                             final String format,
                                             final List<String> bindings) {
            final List<String> substituted = bindings.stream()
                .map(binding -> {
                        if (binding.equals("$${containerName}")) {
                            return containerName;
                        } else {
                            return binding;
                        }
                    })
                .collect(Collectors.toList());
            return String.format(format, substituted.stream().toArray(String[]::new));
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
    private final NsqLookupNode lookup;

    public NsqDockerCluster(final DockerClient dockerClient,
                            final List<NsqdNode> nsqds,
                            final NsqLookupNode lookup)  {
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

    public final NsqLookupNode getLookupNode() {
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
