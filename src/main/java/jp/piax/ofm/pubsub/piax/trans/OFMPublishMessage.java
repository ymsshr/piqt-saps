package jp.piax.ofm.pubsub.piax.trans;

import java.io.Serializable;

import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.impl.NestedMessage;

/**
 * Openflow multicast 用 publish メッセージ
 */
public class OFMPublishMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final StringKey topic;
    protected final Object message;
    protected final String userId;
    protected final long publishId;

    /**
     * コンストラクタ
     * @param topickey OFM topic key
     * @param userid publisher のユーザID
     * @param publishid publish メッセージの publish ID
     * @param message publish メッセージ
     */
    public OFMPublishMessage(StringKey topickey, String userid, long publishid,
            NestedMessage message) {
        if (topickey == null)
            throw new NullPointerException("topickey should not be null");
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");
        if (message == null)
            throw new NullPointerException("message should not be null");

        this.topic = topickey;
        this.message = message;
        this.userId = userid;
        this.publishId = publishid;
    }

    public StringKey getTopic() {
        return topic;
    }

    public Object getMessage() {
        return message;
    }

    public String getUserId() {
        return userId;
    }

    public long getPublishId() {
        return publishId;
    }
}
