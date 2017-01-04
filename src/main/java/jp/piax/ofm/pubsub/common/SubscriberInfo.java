package jp.piax.ofm.pubsub.common;

import java.io.Serializable;
import java.net.InetSocketAddress;

import jp.piax.ofm.gate.messages.IPMACPair;

import org.piax.agent.AgentId;
import org.piax.common.PeerId;

/**
 * Subscriber の情報
 */
public class SubscriberInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private PeerId peerId;      // subscriber の PeerId
    private AgentId agentId;    // subscriber の AgentId
    private String userId;      // subscriber の UserID
    private String topic;       // 要求された topic 名
    private IPMACPair ofmReceiverAddress;   // subscriber の OFM 受信アドレス
    private InetSocketAddress ofmAddress;   // topic に対応する OFM アドレス
    private TrafficStatistics statistics;   // topic に対応する受信時統計情報
    private long dcDelay = -1;    // discoveryCall での応答遅延時間 (ms)

    /**
     * コンストラクタ
     * @param peerid subscriber の PeerId
     * @param agentid subscriber の AgentId
     * @param userid subscriber の UserID
     * @param topic 
     * @param ofmrecvaddress subscriber の OFM 受信アドレス
     * @param ofmaddress topic に対応する OFM アドレス 未設定の場合は null
     * @param statistics topic に対応する受信時統計情報
     */
    public SubscriberInfo(PeerId peerid, AgentId agentid, String userid,
            String topic, IPMACPair ofmrecvaddress, InetSocketAddress ofmaddress,
            TrafficStatistics statistics) {
        if (peerid == null)
            throw new NullPointerException("peerid should not be null");
        if (agentid == null)
            throw new NullPointerException("agentid should not be null");
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (ofmrecvaddress == null)
            throw new NullPointerException("ofmrecvaddress should not be null");
        if (statistics == null)
            throw new NullPointerException("statistics should not be null");

        this.peerId = peerid;
        this.agentId = agentid;
        this.userId = userid;
        this.topic = topic;
        this.ofmReceiverAddress = ofmrecvaddress;
        this.ofmAddress = ofmaddress;
        this.statistics = statistics;
    }

    /**
     * subscriber の PeerId を取得する
     * @return
     */
    public PeerId getPeerId() {
        return peerId;
    }

    /**
     * subscriber の AgentId を取得する
     * @return
     */
    public AgentId getAgentId() {
        return agentId;
    }

    /**
     * subscriber の UserID を取得する
     * @return
     */
    public String getUserId() {
        return userId;
    }

    /**
     * topic 名を取得する
     * @return
     */
    public String getTopic() {
        return topic;
    }

    /**
     * subscriber の OFM 受信アドレス
     * @return
     */
    public IPMACPair getOfmReceiverAddress() {
        return ofmReceiverAddress;
    }

    /**
     * topic に対応する OFM アドレスを取得する
     * @return topic に対応する OFM アドレス 未設定の場合は null
     */
    public InetSocketAddress getOfmAddress() {
        return ofmAddress;
    }

    /**
     * topic に対応する受信時統計情報
     * @return
     */
    public TrafficStatistics getStatistics() {
        return statistics;
    }

    /**
     * discoveryCall での応答遅延時間を設定する
     * @param delay discoveryCall での応答遅延時間 (ms)
     */
    public void setDcDelay(long delay) {
        dcDelay = delay;
    }

    /**
     * discoveryCall での応答遅延時間を取得する
     * 負数の場合は未設定
     * @return discoveryCall での応答遅延時間 (ms)
     */
    public long getDcDelay() {
        return this.dcDelay;
    }
}
