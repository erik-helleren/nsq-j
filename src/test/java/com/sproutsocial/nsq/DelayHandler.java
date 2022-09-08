package com.sproutsocial.nsq;

public class DelayHandler extends TestMessageHandler {
    private final long sleepMillis;

    public DelayHandler(long sleepMillis) {
        super((int) (sleepMillis*2.5));
        this.sleepMillis = sleepMillis;
    }

    @Override
    public void accept(Message msg) {
        super.accept(msg);
        Util.sleepQuietly(sleepMillis);
    }
}
