package com.sproutsocial.nsq;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.sproutsocial.nsq.TestBase.messages;

public class PublisherFocusedDockerTestIT extends BaseDockerTestIT {
    private Subscriber subscriber;
    private Publisher publisher;
    private TestMessageHandler handler;


    @Override
    public void setup() {
        super.setup();
        Util.sleepQuietly(500);
        handler = new TestMessageHandler();
        subscriber = new Subscriber(1, cluster.getLookupNode().getHttpHostAndPort().toString());
        subscriber.subscribe(topic, "tail" + System.currentTimeMillis() , handler);

    }

    @Override
    public void teardown() throws InterruptedException {
        subscriber.stop();
        if (publisher != null) {
            publisher.stop();
        }
        super.teardown();
    }

    @Test
    public void testBasicRoundTrip_noBackup() {
        Publisher publisher = primaryOnlyPublisher();
        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void testBasicRoundTrip_noBackup_primaryNsqDown_waitBeforeTryingAgain() {

        publisher = primaryOnlyPublisher();
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        List<String> messages = messages(20, 40);

        long startTimeMillis = System.currentTimeMillis();
        Assert.assertThrows(NSQException.class, () -> send(topic, messages, 0.5f, 10, publisher));
        long totalTimeMillis = System.currentTimeMillis() - startTimeMillis;
        Assert.assertTrue("Waited at least 10 seconds", totalTimeMillis > TimeUnit.SECONDS.toMillis(10));

    }

    @Test
    public void testBasicRoundTrip_noBackup_primaryNsqDownThenRecovers() {
        publisher = primaryOnlyPublisher();
        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));
        scheduledExecutorService.schedule(() -> cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0)), 1, TimeUnit.SECONDS);

        sendAndVerifyMessagesFromPrimary(publisher, handler);

    }

    @Test
    public void testBasicRoundTrip_WithBackup_noFailure() {
        publisher = backupPublisher();
        sendAndVerifyMessagesFromPrimary(publisher, handler);

    }

    @Test
    public void withBackup_failureAfterSomeMessagesArePublished() {
        publisher = backupPublisher();
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);
    }

    @Test
    public void withBackup_failoverAndFailBackProactivly() {
        publisher = backupPublisher();
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));

        Util.sleepQuietly(TimeUnit.SECONDS.toMillis(35));

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }

    @Test
    public void withBackup_failoverAndFailbackRightAwayIfBackupGoesDown(){
        publisher = backupPublisher();
        sendAndVerifyMessagesFromPrimary(publisher, handler);

        cluster.disconnectNetworkFor(cluster.getNsqdNodes().get(0));

        sendAndVerifyMessagesFromBackup(publisher, handler);

        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(0));
        cluster.reconnectNetworkFor(cluster.getNsqdNodes().get(1));

        sendAndVerifyMessagesFromPrimary(publisher, handler);
    }


}
