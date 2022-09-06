package com.sproutsocial.nsq;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.ServerSocket;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.okhttp.OkDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.ExposedPort;

public class NsqDockerCluster {
    public static class ContainerConfig {
        public String image;
        public String nameFormat;
        public String cmdFormat;

        public ContainerConfig(final String image,
                               final String nameFormat,
                               final String cmdFormat) {
            this.image = image;
            this.nameFormat = nameFormat;
            this.cmdFormat = cmdFormat;
        }
    }

    public static class CreatedContainer {
        public String containerId;
        public String name;
        public Map<String, PortBinding> exposedPortsByName;

        public CreatedContainer(final String containerId,
                                final String name,
                                final Map<String, PortBinding> exposedPortsByName) {
            this.containerId = containerId;
            this.name = name;
            this.exposedPortsByName = exposedPortsByName;
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

        public final List<String> getAllContainerIds() {
            final ImmutableList.Builder<String> containerIds = new ImmutableList.Builder<>();
            containerIds.add(lookupNode.containerId);
            for (final NsqdNode nsqd : nsqdNodes) {
                containerIds.add(nsqd.containerId);
            }
            return containerIds.build();
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

        public Builder withLookupConfig(final ContainerConfig config) {
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
                executor,
                dockerClient,
                containers.nsqdNodes,
                containers.lookupNode);
        }

        private CreatedContainers createAndStartContainers(final DockerClient dockerClient) {
            final List<Future<CreatedContainer>> lookupContainers = createContainers(
                dockerClient,
                lookupConfig,
                1,
                ImmutableList.of(),
                ImmutableMap.of("tcp_port", ExposedPort.tcp(4160), "http_port", ExposedPort.tcp(4161)));
            final ImmutableList.Builder<NsqdNode> nsqdNodes = new ImmutableList.Builder<>();
            final NsqLookupNode lookupNode;
            try {
                final List<Future<CreatedContainer>> nsqdContainers = createContainers(
                    dockerClient,
                    nsqdConfig,
                    nsqdCount,
                    ImmutableList.of(String.format(
                                         "%s:%d", lookupContainers.get(0).get().name, 4160),// Lookup TCP hostname and port
                                     "$${containerName}"), // What address this nsqd is going to broadcast to the lookup
                    ImmutableMap.of("tcp_port", ExposedPort.tcp(4150)));

                lookupNode = new NsqLookupNode(lookupContainers.get(0).get().containerId, HostAndPort.fromParts("127.0.0.1", 4151));

                for (final Future<CreatedContainer> createdNsqd : nsqdContainers) {
                    // TODO: Fix hardcoded hostname and port, after we setup port forwarding
                    nsqdNodes.add(new NsqdNode(createdNsqd.get().containerId, HostAndPort.fromParts("127.0.0.1", 4151)));
                }
                final CreatedContainers created = new CreatedContainers(nsqdNodes.build(), lookupNode);
                startContainers(dockerClient, created.getAllContainerIds());
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
                                                                      final List<String> cmdBindings,
                                                                      final Map<String, ExposedPort> exposedPorts) {
            final ImmutableList.Builder<Future<CreatedContainer>> createdContainers = new ImmutableList.Builder<>();
            for (int i = 0; i < count; i++) {
                final String containerName = String.format(config.nameFormat, i);
                final List<String> cmd = buildCmd(containerName, config.cmdFormat, cmdBindings);
                final Map<String, PortBinding> allocatedPorts = exposedPorts.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                                 entry -> entry.getKey(),
                                 entry -> new PortBinding(Ports.Binding.bindPort(randomPort()), entry.getValue())));
                createdContainers.add(executor.submit(() -> {
                            final CreateContainerResponse response = dockerClient.createContainerCmd(config.image)
                                .withName(containerName)
                                .withCmd(cmd)
                                .withPortBindings(allocatedPorts.values().stream().toArray(PortBinding[]::new))
                                .exec();
                            return new CreatedContainer(response.getId(), containerName, allocatedPorts);
                        }));
            }
            return createdContainers.build();
        }

        private final void startContainers(final DockerClient dockerClient, final List<String> containerIds) {
            NsqDockerCluster.executeForAllContainers(
                executor, containerIds,
                containerId -> dockerClient.startContainerCmd(containerId).exec());
        }

        private final List<String> buildCmd(final String containerName,
                                            final String format,
                                            final List<String> bindings) {
            final List<String> substituted = bindings.stream()
                .map(binding -> binding.replace("$${containerName}", containerName))
                .collect(Collectors.toList());
            return Splitter.on(" ")
                .splitToList(String.format(format, substituted.stream().toArray(String[]::new)));
        }

        // Get a random, locally available port allocated to us.
        private final int randomPort() {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(0);
                return serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (serverSocket != null) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
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

    private final ExecutorService executor;
    private final DockerClient dockerClient;
    private final List<NsqdNode> nsqds;
    private final NsqLookupNode lookup;

    public NsqDockerCluster(final ExecutorService executor,
                            final DockerClient dockerClient,
                            final List<NsqdNode> nsqds,
                            final NsqLookupNode lookup)  {
        this.executor = executor;
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

    public final List<String> getAllContainerIds() {
        final ImmutableList.Builder<String> containerIds = new ImmutableList.Builder<>();
        containerIds.add(lookup.containerId);
        for (final NsqdNode nsqd : nsqds) {
            containerIds.add(nsqd.containerId);
        }
        return containerIds.build();
    }

    public void shutdown() {
        stopContainers();
        removeContainers();
    }

    public void disconnectNetworkFor(final ConnectableNode node) {
        // TODO: Cut off the host from the underlying docker network
    }

    public void reconnectNetworkFor(final ConnectableNode node) {
        // TODO: Re-connect the host from the underlying docker network
    }

    private void stopContainers() {
        executeForAllContainers(
            executor, getAllContainerIds(),
            containerId -> dockerClient.stopContainerCmd(containerId).exec());
    }

    private void removeContainers() {
        executeForAllContainers(
            executor, getAllContainerIds(),
            containerId -> dockerClient.removeContainerCmd(containerId).exec());
    }

    private static void executeForAllContainers(final ExecutorService executor,
                                                final List<String> containerIds,
                                                final Consumer<String> executeFn) {
        final List<Callable<Void>> tasks = containerIds.stream()
            .map(containerId -> new Callable<Void>() {
                    @Override
                    public Void call() {
                        executeFn.accept(containerId);
                        return null;
                    }
                })
            .collect(Collectors.toList());

        try {
            final List<Future<Void>> futures = executor.invokeAll(tasks);
            for (final Future<Void> f : futures) {
                // Block until the task is done.
                f.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during container execution");
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
