package jp.piax.ofm.pubsub.common;

import java.io.IOException;

public class PubSubException extends IOException {
    private static final long serialVersionUID = 1L;

    public PubSubException() {
        super();
    }

    public PubSubException(String msg, Throwable t) {
        super(msg, t);
    }

    public PubSubException(String msg) {
        super(msg);
    }

    public PubSubException(Throwable t) {
        super(t);
    }

}
