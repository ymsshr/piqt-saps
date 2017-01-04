package jp.piax.ofm.pubsub;

import java.util.List;

import jp.piax.ofm.pubsub.common.PubSubException;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf;

import org.piax.agent.AgentException;
import org.piax.agent.AgentHome;
import org.piax.agent.AgentId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserPubSubImpl implements UserPubSub {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(UserPubSubImpl.class);

    private String userId = null;
    private AgentId userAgentId = null;
    private AgentHome home = null;
    private PubSubAgentIf stub = null;

    public UserPubSubImpl(String userid, AgentId useragentid, AgentHome home) throws AgentException {
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");
        if (useragentid == null)
            throw new IllegalArgumentException("useragentid should not be null");
        if (home == null)
            throw new IllegalArgumentException("home should not be null");

        this.userId = userid;
        this.userAgentId = useragentid;
        this.home = home;

        stub = home.getStub(null, userAgentId);
    }

    @Override
    public String getUserId() {
        return stub.getUserId();
    }

    @Override
    public void publish(String topic, String content) throws PubSubException {
        logger.trace("publish [{}] {}, {}", userId, topic, content);
        try {
            stub.publish(topic, content);
        } catch (IllegalArgumentException | NoSuchOverlayException
                | IncompatibleTypeException e) {
            throw new PubSubException("publish failed", e);
        }
    }

    @Override
    public void subscribe(String topic) throws PubSubException {
        logger.trace("subscribe [{}] {}", userId, topic);
        try {
            stub.subscribe(topic);
        } catch (IllegalArgumentException | IncompatibleTypeException
                | NoSuchOverlayException e) {
            throw new PubSubException("publish failed", e);
        }
    }

    @Override
    public void unsubscribe(String topic) throws PubSubException {
        logger.trace("unsubscribe [{}] {}", userId, topic);
        stub.unsubscribe(topic);
    }

    @Override
    public void setMessageListener(final MessageListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("listener should not be null");

        stub.setCallbackListener(new PubSubAgentIf.OnMessageListener() {
            @Override
            public void onReceive(String sender, String topic, String content) {
                logger.trace("onReceive [{}] from {} {}, {}", userId, sender, topic, content);
                listener.onMessage(sender, topic, content);
            }
        });
    }

    @Override
    public List<String> getSubscribedTopic() {
        return stub.getSubscribedTopic();
    }
}
