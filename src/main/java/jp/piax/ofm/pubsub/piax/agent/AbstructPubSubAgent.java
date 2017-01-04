package jp.piax.ofm.pubsub.piax.agent;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.common.NullAddress;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.common.DuplicatedMessageChecker;
import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.common.TrafficStatistics;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.misc.SubscriberCounter;
import jp.piax.ofm.pubsub.misc.TrafficCounter;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;

import org.piax.agent.AgentId;
import org.piax.agent.MobileAgent;
import org.piax.common.PeerId;
import org.piax.common.TransportIdPath;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.RemoteCallable;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstructPubSubAgent extends MobileAgent implements PubSubAgentIf {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(AbstructPubSubAgent.class);

    /** ユーザID */
    protected String userId = "";
    /** 受信時統計情報カウンタタイムウィンドウ (ms) */
    protected long counterTimewindow = PubSubAgentConfigValues.TrafficCounterTimewindow;
    /** メッセージ重複受信検出用キューの長さ */
    protected int duplicatedMessageQueueLength = PubSubAgentConfigValues.DuplicatedMessageQueueLength;


    protected transient TransportIdPath pubsubIdPath;     // PubSub 用 DOLR オーバレイの TransportIdPath
    protected transient IPMACPair ofmReceiverAddress;     // OFM 受信アドレス
    protected transient OnMessageListener listener = null;    // メッセージ受信イベントリスナ

    public AtomicLong publishIdCounter = new AtomicLong(0);  // Publish ID カウンタ
    public Set<String> subscribedTopic = new HashSet<>();    // subscribe 中のトピック

    protected transient DuplicatedMessageChecker duplicatedChecker; // 重複受信メッセージ判定クラス
    protected transient TrafficCounter counter; // 受信時統計情報カウンタ

    protected transient ScheduledExecutorService scheduledExecutor; // 定期処理実行用共用ThreadPool
    public transient SubscriberCounter subscribedTopicCounter;   // ピア上での subscribe 数カウンタ
    protected transient OFMAddressCache ofmAddressCache;            // OFM address キャッシュ

    public AbstructPubSubAgent() {
        counter = new TrafficCounter(counterTimewindow);
        duplicatedChecker = new DuplicatedMessageChecker(duplicatedMessageQueueLength);
    }

    @Override
    public void onCreation() {
        // 共有情報取得
        pubsubIdPath = ((PubSubAgentHomeImpl) this.getHome()).getOFMPubSubIdPath();
        ofmReceiverAddress = ((PubSubAgentHomeImpl) this.getHome()).getOFMReceiverAddress();
        subscribedTopicCounter = ((PubSubAgentHomeImpl) this.getHome()).getSubscribedTopicCounter();
        scheduledExecutor = ((PubSubAgentHomeImpl) this.getHome()).getScheduledExecutor();
        ofmAddressCache = ((PubSubAgentHomeImpl) this.getHome()).getOFMAddressCache();
    }

    @Override
    public void onArrival() {
        // デシリアライズ時はコンストラクタが呼ばれないためここで初期化
        counter = new TrafficCounter(counterTimewindow);
        duplicatedChecker = new DuplicatedMessageChecker(duplicatedMessageQueueLength);

        // 共有情報取得
        pubsubIdPath = ((PubSubAgentHomeImpl) this.getHome()).getOFMPubSubIdPath();
        ofmReceiverAddress = ((PubSubAgentHomeImpl) this.getHome()).getOFMReceiverAddress();
        subscribedTopicCounter = ((PubSubAgentHomeImpl) this.getHome()).getSubscribedTopicCounter();
        scheduledExecutor = ((PubSubAgentHomeImpl) this.getHome()).getScheduledExecutor();
        ofmAddressCache = ((PubSubAgentHomeImpl) this.getHome()).getOFMAddressCache();

        // TODO listener 再設定（どうやって？）
    }

    @Override
    public void onDestruction() {
        // nothing todo
    }


    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#getSubscribedTopic()
     */
    @Override
    public List<String> getSubscribedTopic() {
        return new ArrayList<String>(subscribedTopic);
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#setCallbackListener(jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf.OnMessageListener)
     */
    @Override
    public void setCallbackListener(OnMessageListener listener) {
        this.listener = listener;
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#setUserId(java.lang.String)
     */
    @Override
    public void setUserId(String userid) {
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");

        logger.debug("[{}] setUserId {}", userid, userid);

        this.userId = userid;
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#getUserId()
     */
    @Override
    public String getUserId() {
        return userId;
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#getSubscriberInfo(java.lang.String)
     */
    @Override
    @RemoteCallable
    public SubscriberInfo getSubscriberInfo(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("[{}] getSubscriberInfo [{}]", getUserId(), topic);

        PeerId pid = this.getHome().getPeerId();
        AgentId aid = this.getId();
        String userid = this.getUserId();
        TrafficStatistics stat = counter.getStatistics(topic);
        InetSocketAddress ofmaddress = ofmAddressCache.get(topic);
        return new SubscriberInfo(pid, aid, userid, topic, ofmReceiverAddress, ofmaddress, stat);
    }


    /**
     * OFGate にトピック名に対応する OFM アドレスを問い合わせる
     * @param topic トピック名
     * @return OFM アドレス (topic に対応するOFM アドレスが無い場合は null)
     */
    public InetSocketAddress queryOFMAddress(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("[{}] queryOFMAddress topic:[{}]", getUserId(), topic);

        InetSocketAddress ofmaddress = null;
        FutureQueue<?> fq = discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "getOFMAddress", topic);
        for (RemoteValue<?> remote : fq) {
            if (remote.getException() == null) {
                if (remote.getValue() != null) {
                    try {
                        ofmaddress = (InetSocketAddress) remote.getValue();
                        // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
                        // null の代わりに NULL object が返る
                        if (NullAddress.NULL_ADDR.equals(ofmaddress)) {
                            logger.debug("discoveryCall to getOFMAddress returns NULL_ADDR from {}", remote.getPeer());
                            ofmaddress = null;
                        }
                    } catch (ClassCastException e) {
                        logger.error("discoveryCall to getOFMAddress returns illegal class : {}", remote.getValue().getClass());
                    }
                } else {
                    logger.debug("discoveryCall to getOFMAddress returns null from {}", remote.getPeer());
                }
            } else {
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                        getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                logger.error(remote.getException().getMessage(), remote.getException());
            }
        }
        logger.debug("[{}] queryOFMAddress returns {}", getUserId(), ofmaddress);
        return ofmaddress;
    }

    /**
     * OFGate に topic に対応する OFM メンバへの追加を要求する
     * @param topic topic名
     * @return OFM アドレス (topic に対応するOFM アドレスが無い場合は null)
     */
    protected InetSocketAddress notifySubscribeToOFGate(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("[{}] notifySubscribeToOFGate topic:[{}]", getUserId(), topic);

        InetSocketAddress ofmaddress = null;
        FutureQueue<?> fq = discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "addSubscriber", topic, ofmReceiverAddress);
        for (RemoteValue<?> remote : fq) {
            if (remote.getException() == null) {
                // topic に対する OFM アドレスがあれば保存
                if (remote.getValue() != null) {
                    try {
                        // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
                        // null の代わりに NULL object が返る
                        ofmaddress = (InetSocketAddress) remote.getValue();
                        if (NullAddress.NULL_ADDR.equals(ofmaddress)) {
                            logger.debug("[{}] addSubscriber for [{}] returns NULL_ADDR from {}", getUserId(), remote.getPeer());
                            ofmaddress = null;
                        }
                    } catch (ClassCastException e) {
                        logger.error("[{}] discoveryCall to addSubscriber returns illegal class : {}", getUserId(), remote.getValue().getClass());
                    }
                } else {
                    logger.debug("[{}] discoveryCall addSubscriber returns null from {}", getUserId(), remote.getPeer());
                }
            } else {
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                        getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                logger.error(remote.getException().getMessage(), remote.getException());
            }
            break;
        }
        logger.debug("[{}] notifySubscribeToOFGate returns {}", getUserId(), ofmaddress);
        return ofmaddress;
    }

    /**
     * OFGate に topic に対応する OFM メンバからの離脱を要求する
     * @return OFM メンバ離脱後も OFM アドレスが有効な場合は OFM アドレス、無効化された場合は null
     * @param topic topic名
     */
    protected InetSocketAddress notifyUnsubscribeToOFGate(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("[{}] notifyUnubscribeToOFGate topic:[{}]", getUserId(), topic);

        InetSocketAddress ofmaddress = null;
        FutureQueue<?> fq = discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "removeSubscriber", topic, ofmReceiverAddress);
        for (RemoteValue<?> remote : fq) {
            if (remote.getException() == null) {
                // topic に対する OFM アドレスがあれば保存
                if (remote.getValue() != null) {
                    try {
                        // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
                        // null の代わりに NULL object が返る
                        ofmaddress = (InetSocketAddress) remote.getValue();
                        if (NullAddress.NULL_ADDR.equals(ofmaddress)) {
                            logger.debug("[{}] removeSubscriber for [{}] returns NULL_ADDR from {}", getUserId(), topic, remote.getPeer());
                            ofmaddress = null;
                        }
                    } catch (ClassCastException e) {
                        logger.error("[{}] discoveryCall to removeSubscriber returns illegal class : {}", getUserId(), remote.getValue().getClass());
                    }
                } else {
                    logger.debug("[{}] discoveryCall removeSubscriber returns null from {}", getUserId(), remote.getPeer());
                }
            } else {
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                        getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                logger.error(remote.getException().getMessage(), remote.getException());
            }
            break;
        }
        logger.debug("[{}] notifyUnubscribeToOFGate returns {}", getUserId(), ofmaddress);
        return ofmaddress;
    }

    private Object piaxAttribLock = new Object();

    /**
     * rowattribs に対応する PIAX の属性を登録する
     * @param rowattribs
     */
    protected void _declaredAttributes(String[] rowattribs) {
        synchronized (piaxAttribLock) {
            // 未登録であれば登録する
            for (String key : rowattribs) {
                if (!this.getHome().getDeclaredAttribNames().contains(key)) {
                    this.getHome().declareAttrib(key, String.class);
                }
            }
        }
    }

    /**
     * rowattribs にオーバレイを対応づける
     * @param rowattribs 
     */
    protected void _bindSubscribeOverlays(String[] rowattribs) {
        synchronized (piaxAttribLock) {
            _declaredAttributes(rowattribs);
            for (String key : rowattribs) {
                try {
                    if (this.getHome().getBindOverlay(key) == null) {
                            this.getHome().bindOverlay(key, pubsubIdPath);
                    }
                } catch (IllegalArgumentException | NoSuchOverlayException
                        | IncompatibleTypeException e) {
                    // ここで catch される場合は pubsubIdPath の設定間違いの可能性が大
                    logger.error("[{}] Fatal exception. maybe bad pubsubIdPath setting : {}", getUserId(), pubsubIdPath);
                    logger.error("",e);
                }
            }
        }
    }
}