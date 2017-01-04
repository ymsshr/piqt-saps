package jp.piax.ofm.gate.messages;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Topic と addr:port のペアを持つデータクラス
 */
public class TopicAddressPair implements Serializable {
    private static final long serialVersionUID = 1L;

    private String topic;   // topic 名
    private InetSocketAddress address = null;   // アドレス

    /**
     * コンストラクタ
     * 
     * @param topic topic 名
     * @param address 
     */
    public TopicAddressPair(String topic, InetSocketAddress address) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        this.topic = topic;
        this.address = address;
    }

    /**
     * topic 名を取得する
     * @return topic 名
     */
    public String getTopic() {
        return topic;
    }

    /**
     * アドレスを取得する
     * @return
     */
    protected InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicAddressPair other = (TopicAddressPair) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }
}
