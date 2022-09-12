package com.sproutsocial.nsq;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.sproutsocial.nsq.TestBase.messages;
import static com.sproutsocial.nsq.TestBase.random;
import static org.slf4j.LoggerFactory.getLogger;

public class BaseDockerTestIT {
    protected NsqDockerCluster cluster;
    protected String topic;
    protected ScheduledExecutorService scheduledExecutorService;
    private static final Logger LOGGER = getLogger(BaseDockerTestIT.class);
    @Before
    public void setup() {
        cluster = NsqDockerCluster.builder()
                .withNsqdCount(3)
                .start();

        topic = "topic" + System.currentTimeMillis();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() throws InterruptedException {

        cluster.shutdown();

        scheduledExecutorService.shutdown();
        scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    protected void send(String topic, List<String> msgs, double delayChance, int maxDelay, Publisher publisher) {
        int count = 0;
        LOGGER.info("Sending {} messags to topic {}", msgs.size(), topic);
        for (String msg : msgs) {
            if (random.nextFloat() < delayChance) {
                Util.sleepQuietly(random.nextInt(maxDelay));
            }
            publisher.publish(topic, msg.getBytes());
            if (++count % 10 == 0) {
                System.out.println("sent " + count + " msgs");
            }
        }
    }

    protected void sendAndVerifyMessagesFromBackup(Publisher publisher, TestMessageHandler handler) {
        List<String> postFailureMessages = messages(20, 40);
        send(topic, postFailureMessages, 0.5, 10, publisher);
        List<NSQMessage> receivedMessages = handler.drainMessagesOrTimeOut(postFailureMessages.size());
        validateReceivedAllMessages(postFailureMessages, receivedMessages, true);
        validateFromParticularNsqd(receivedMessages, 1);
    }

    protected void sendAndVerifyMessagesFromPrimary(Publisher publisher, TestMessageHandler handler) {
        List<String> messages = messages(20, 40);
        send(topic, messages, 0.5f, 10, publisher);
        List<NSQMessage> preFailureActual = handler.drainMessagesOrTimeOut(messages.size());
        validateReceivedAllMessages(messages, preFailureActual, true);
        validateFromParticularNsqd(preFailureActual, 0);
    }

    private void validateFromParticularNsqd(List<NSQMessage> receivedMessages, int nsqHostIndex) {
        for (NSQMessage e : receivedMessages) {
            Assert.assertEquals(cluster.getNsqdNodes().get(nsqHostIndex).getTcpHostAndPort(), e.getConnection().getHost());
        }
    }

    protected Publisher primaryOnlyPublisher() {
        return new Publisher(cluster.getNsqdNodes().get(0).getTcpHostAndPort().toString());
    }

    protected Publisher backupPublisher() {
        return new Publisher(cluster.getNsqdNodes().get(0).getTcpHostAndPort().toString(), cluster.getNsqdNodes().get(1).getTcpHostAndPort().toString());
    }

    public void validateReceivedAllMessages(List<String> expected, List<NSQMessage> actual, boolean validateOrder) {
        List<String> actualMessages = actual.stream().map(m -> new String(m.getData())).collect(Collectors.toList());
        List<String> expectedCopy = new ArrayList<>(expected);
        if (!validateOrder) {
            Collections.sort(actualMessages);
            Collections.sort(expectedCopy);
        }
        Assert.assertArrayEquals(expected.toArray(), actualMessages.toArray());
    }
}
