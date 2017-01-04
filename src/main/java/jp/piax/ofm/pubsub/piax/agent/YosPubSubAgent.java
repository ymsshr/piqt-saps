package jp.piax.ofm.pubsub.piax.agent;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.common.NullAddress;
import jp.piax.ofm.gate.common.SetupOFMResult;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.common.PublishMessage.PublishPath;
import jp.piax.ofm.pubsub.common.TransPathInfo;
import jp.piax.ofm.pubsub.common.YosPubSubAgentConfigValues;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.YosPubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.YosSimpleMonitor;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;

import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.NoSuchOverlayException;
import org.piax.gtrans.RemoteValue;
import org.piax.util.MersenneTwister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YosPubSubAgent extends AbstructPubSubAgent implements PubSubAgentIf {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(YosPubSubAgent.class);

    public static final String OFM_TOPIC_PREFIX = "$OFMTOPIC$.";

    /** OFM 判定周期兼 Subscriber 統計情報収集周期 (ms) */
    private long pollingPeriod = PubSubAgentConfigValues.SubscriberPollingPeriod;
    /** OpenFlow Multicast 時のメッセージ loopback 待ちタイムアウト (ms) */
    private long recvLoopbackTimeout = PubSubAgentConfigValues.RecvLoopbackTimeout;

    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでのウエイト
     * migrationDelayBase + rnd(migrationDelayAdditionalWidth) ms 後に移行する
     */
    private long migrationDelayBase = YosPubSubAgentConfigValues.OFMtopicMigrationDelayBase;
    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでのウエイト
     * migrationBaseDelay + rnd(migrationDelayAdditionalWidth) ms 後に移行する
     */
    private long migrationDelayAdditionalWidth = YosPubSubAgentConfigValues.OFMtopicMigrationDelayAdditionalWidth;

    private Random rndsrc = new MersenneTwister();

    /** subscribe のメッセージ受信経路の状態 */
    enum ListenState {
        /** ALM 経由のメッセージ受信 (topic + OFM)*/
        Overlay,
        /** OFM に移行中 */
        OFMMigrating,
        /** OFM 経由のメッセージ受信 (OFM + OFMtopic) */
        OFM
    }

    /** subscribe している topic とその受信経路の状態 */
    private ConcurrentHashMap<String, ListenState> subscribedTopic = new ConcurrentHashMap<>();

    private volatile YosPubSubMonitor monitor = new YosSimpleMonitor();
    private ScheduledFuture<?> monitoringTask;      // モニタリングタスク

    // モニタリング処理
    private Runnable monitoring = new Runnable() {
        @Override
        public void run() {
            logger.debug("[{}] Start monitoring task", getUserId());
            for (String topic : monitor.getMonitoringTopics()) {
                logger.info("[{}] Start monitoring for topic:[{}]", getUserId(), topic);
                try {
                    // subscriber の情報を収集
                    FutureQueue<?> fq = discoveryCallAsync(topic +" eq \"" + topic + "\"", "getSubscriberInfo", topic);

                    Set<SubscriberInfo> info = new HashSet<SubscriberInfo>();
                    for (RemoteValue<?> remote : fq) {
                        if (remote.getException() == null) {
                            if (remote.getValue() != null) {
                                info.add((SubscriberInfo) remote.getValue());
                            } else {
                                logger.error("[{}] discoveryCall getSubscriberInfo retuens null from {}", getUserId(), remote.getPeer());
                            }
                        } else {
                            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                            logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                                    getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                            logger.error(remote.getException().getMessage(), remote.getException());
                        }
                    }
                    logger.info("[{}] get subscriber information for topic:[{}] from {} subscribers", getUserId(), topic, info.size());
                    monitor.putSubscriberInfo(topic, info);

                    // OFM 網構築判定
                    if (monitor.setupOFM(topic)) {
                        Set<IPMACPair> subscribers = monitor.getSubscriberAddress(topic);
                        if (subscribers != null && !subscribers.isEmpty()) {
                            logger.info("[{}] try setupOFM for topic:[{}]", getUserId(), topic);
                            // OFM 網構築
                            fq = discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "setupOFM", topic, subscribers);
                            for (RemoteValue<?> remote : fq) {
                                if (remote.getException() == null) {
                                    if (remote.getValue() != null) {
                                        SetupOFMResult result = (SetupOFMResult) remote.getValue();
                                        if (result != null && result.getOfmAddress() != null) {
                                            // 逐次切替方式では、リクエストした subscriber が既存の OFM メンバと合致していなくと
                                            // も、しばらくすれば OFMメンバに取り込まれることが期待できるため !fullySucceed でも成功と見なす
                                            // （既にtopicが登録済みだった場合）でも topic の OFMアドレスをモニタに渡す
                                            monitor.setOFMAddress(topic, result.getOfmAddress());
                                            logger.info("[{}] setupOFM succeeded for topic:[{}]. OFM address: {}", getUserId(), topic, result.getOfmAddress());
                                        } else {
                                            logger.warn("[{}] setupOFM failed for topic:[{}].", getUserId(), topic);
                                        }
                                    } else {
                                        logger.debug("[{}] setupOFM for topic:[{}] returns null from {}", getUserId(), topic, remote.getPeer());
                                    }
                                } else {
                                    StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                                    logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                                            getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                                    logger.error(remote.getException().getMessage(), remote.getException());
                                }
                                break;
                            }
                        } else {
                            logger.debug("[{}] no subscribers of topic:[{}]", getUserId(), topic);
                        }
                    } else if (monitor.addSubscriberToOFM(topic)) {
                        Set<IPMACPair> subscribers = monitor.getSubscriberAddress(topic);
                        if (subscribers != null && !subscribers.isEmpty()) {
                            logger.info("[{}] try addSubscriber for [{}]", getUserId(), topic);
                            // ALM topic を subscribe している subscriber を OFM メンバーに追加する
                            fq = discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "addSubscribers", topic, subscribers);
                            for (RemoteValue<?> remote : fq) {
                                if (remote.getException() == null) {
                                    if (remote.getValue() != null) {
                                        InetSocketAddress ofmaddress = (InetSocketAddress) remote.getValue();
                                        // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
                                        // null の代わりに NULL object が返る
                                        if (NullAddress.NULL_ADDR.equals(ofmaddress)) {
                                            ofmaddress = null;
                                        }
                                        monitor.setOFMAddress(topic, ofmaddress);
                                        logger.info("[{}] addSubscriber succeeded for [{}]. OFM address: {}", getUserId(), topic, ofmaddress);
                                    } else {
                                        logger.info("[{}] addSubscriber for [{}] returns null from {}", getUserId(), topic, remote.getPeer());
                                    }
                                } else {
                                    StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                                    logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                                            getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                                    logger.error(remote.getException().getMessage(), remote.getException());
                                }
                                break;
                            }
                        } else {
                            logger.debug("[{}] no subscribers of topic:[{}]", getUserId(), topic);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    logger.debug("[{}] Finish monitoring for topic [{}]", getUserId(), topic);
                }
            }
            logger.debug("[{}] Finish monitoring task", getUserId());
        }
    };

    @Override
    public void onCreation() {
        super.onCreation();

        try {
            if (PubSubAgentConfigValues.MonitorClazz != null) {
                PubSubMonitor mon = PubSubAgentConfigValues.MonitorClazz.newInstance();
                if (mon instanceof YosPubSubMonitor) {
                    this.monitor = (YosPubSubMonitor) mon;
                } else {
                    throw new IllegalArgumentException("Monitor class should be an instance of YosPubSubMonitor");
                }
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Monitor class cannot new an instance : " + PubSubAgentConfigValues.MonitorClazz.getCanonicalName(), e);
        }

        OFMAddressCache ofmAddressCache = ((PubSubAgentHomeImpl) this.getHome()).getOFMAddressCache();
        monitor.attachOFMAddressCache(ofmAddressCache);

        startMonitoringThread();
    }

    @Override
    public void onDeparture() {
        // 移動前にモニタリングスレッドを終了させる
        try {
            stopMonitoringThread();
        } catch (InterruptedException e) {
            logger.error("[{}] Failed to stop MonitoringThread", getUserId(), e);
        }

        // 移動前に subscribe カウンタを減じ、必要に応じて OFGate に通知
        // これを忘れると、エージェント移動時に OFM メンバから外されないゾンビ subscriber が生じる
        for (String topic : subscribedTopic.keySet()) {
            if (subscribedTopicCounter.unsubscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバから削除要求
                if (notifyUnsubscribeToOFGate(topic) == null) {
                    monitor.setOFMAddress(topic, null); // OFM メンバから削除された結果、 OFM アドレスが無効化された
                }
            }
            // 移動前に ALM 経由に戻す
            String ofmtopic = getOFMtopic(topic);
            if (ofmtopic.equals(this.getAttribValue(ofmtopic))) {
                this.removeAttrib(ofmtopic);
                try {
                    // XXX useIndex は一旦 false にしておき、移動先で true 化するほうが低負荷か？
                    this.setAttrib(topic, topic, true);
                } catch (IllegalArgumentException | IncompatibleTypeException e) {
                    // 通常は生じない
                    logger.error("[{}] Faild to setAttrib [{}]", getUserId(), topic);
                    logger.error(e.getMessage(), e);
                }
            }
            subscribedTopic.replace(topic, ListenState.Overlay);
        }

        super.onDeparture();
    }


    @Override
    public void onArrival() {
        super.onArrival();

        OFMAddressCache ofmAddressCache = ((PubSubAgentHomeImpl) this.getHome()).getOFMAddressCache();
        monitor.attachOFMAddressCache(ofmAddressCache);

        // XXX agent 到着時は ALM 受信状態となる

        // 移動後に subscribe カウンタを加算する
        for (String topic : subscribedTopic.keySet()) {
            subscribedTopicCounter.subscribeAndNeedUpdate(topic);
        }

        // モニタリングスレッドを再始動する
        startMonitoringThread();

        // TODO listener 再設定（どうやって？）
    }


    @Override
    public void onDestruction() {
        // 終了前に subscribe カウンタを減じ、必要に応じて OFGate に通知
        // これを忘れると、エージェント終了時に OFM メンバから外されないゾンビ subscriber が生じる
        for (String topic : subscribedTopic.keySet()) {
            if (subscribedTopicCounter.unsubscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバから削除要求
                if (notifyUnsubscribeToOFGate(topic) == null) {
                    monitor.setOFMAddress(topic, null); // OFM メンバから削除された結果、 OFM アドレスが無効化された
                }
            }
        }

        // モニタリングスレッドを終了させる
        try {
            stopMonitoringThread();
        } catch (InterruptedException e) {
            logger.error("[{}] Failed to stop MonitoringThread", getUserId(), e);
        }
    }

    /**
     * モニタリングスレッドを開始する
     */
    private synchronized void startMonitoringThread() {
        if (monitoringTask == null) {
            monitoringTask = scheduledExecutor.scheduleAtFixedRate(monitoring, pollingPeriod, pollingPeriod, TimeUnit.MILLISECONDS);
            logger.debug("[{}] Start monitoring thread. Monitoring subscribers at {} (ms) interval", getUserId(), pollingPeriod);
        }
    }

    /**
     * モニタリングスレッドを終了する
     * @throws InterruptedException 
     */
    private synchronized void stopMonitoringThread() throws InterruptedException {
        if (monitoringTask != null && !monitoringTask.isDone()) {
            monitoringTask.cancel(true);
            try {
                monitoringTask.get();   // スレッド終了待ち
            } catch (CancellationException e) {
                // 先に cancel しているため、ここを通る
            } catch (ExecutionException e) {
                logger.error("[{}] ExecutionException in monitoring task", e);
            }
            logger.debug("[{}] Stop monitoring thread", getUserId());
        }
        monitoringTask = null;
    }

    /* (非 Javadoc)
     * @see jp.piaxinc.ofm.piax.agent.PubSubAgentIf#getSubscribedTopic()
     */
    @Override
    public List<String> getSubscribedTopic() {
        return new ArrayList<String>(subscribedTopic.keySet());
    }

    /* (非 Javadoc)
     * @see jp.piaxinc.ofm.piax.agent.PubSubAgentIf#subscribe(java.lang.String)
     */
    @Override
    public void subscribe(String topic) throws IllegalArgumentException, IncompatibleTypeException, NoSuchOverlayException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!isValidTopicName(topic))
            throw new IllegalArgumentException("topic '" + topic + "' can not use for topic name");
        if (userId == null)
            throw new IllegalStateException("UserId is not set");

        logger.info("[{}] subscribe topic:[{}]", getUserId(), topic);

        // topic と Overlay を対応づける
        bindSubscribeOverlay(topic);

        synchronized (subscribedTopic) {
            if (subscribedTopic.containsKey(topic)) {
                logger.warn("[{}] Already subscribed", getUserId());
                return;
            }

            // エージェントにtopicを属性として持たせる
            this.setAttrib(topic, topic, true);
            logger.debug("[{}] setAttrib {}", getUserId(), topic);
            this.setAttrib(getOFMKey(topic), getOFMKey(topic), true);
            logger.debug("[{}] setAttrib {}", getUserId(), getOFMKey(topic));

            subscribedTopic.put(topic, ListenState.Overlay);

            // subscribe の参照カウント数を増やす
            // この手法では subscriber から OFGate には通知しない
            subscribedTopicCounter.subscribeAndNeedUpdate(topic);
        }
    }

    /* (非 Javadoc)
     * @see jp.piaxinc.ofm.piax.agent.PubSubAgentIf#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!isValidTopicName(topic))
            throw new IllegalArgumentException("topic '" + topic + "' can not use for topic name");
        if (userId == null)
            throw new IllegalStateException("UserId is not set");

        logger.info("[{}] unsubscribe topic:[{}]", getUserId(), topic);

        String ofmkey = getOFMKey(topic);
        String ofmtopic = getOFMtopic(topic);

        // topicに対応する属性が登録されていない場合は登録する
        declaredAttribute(topic);

        synchronized (subscribedTopic) {
            if (!subscribedTopic.containsKey(topic)) {
                return;
            }

            // エージェントが持っていた属性を削除する
            if (topic.equals(this.getAttribValue(topic))) {
                this.removeAttrib(topic);
                logger.debug("[{}] removeAttrib {}", getUserId(), topic);
            }
            if (ofmkey.equals(this.getAttribValue(ofmkey))) {
                this.removeAttrib(ofmkey);
                logger.debug("[{}] removeAttrib {}", getUserId(), ofmkey);
            }
            if (ofmtopic.equals(this.getAttribValue(ofmtopic))) {
                this.removeAttrib(ofmtopic);
                logger.debug("[{}] removeAttrib {}", getUserId(), ofmtopic);
            }

            subscribedTopic.remove(topic);

            // ピア上で最後に unsubscribe する場合は OFGate に通知
            if (subscribedTopicCounter.unsubscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバから削除要求
                if (notifyUnsubscribeToOFGate(topic) == null) {
                    monitor.setOFMAddress(topic, null); // OFM メンバから削除された結果、 OFM アドレスが無効化された
                }
            }
        }
    }

    /* (非 Javadoc)
     * @see jp.piaxinc.ofm.piax.agent.PubSubAgentIf#publish(java.lang.String, java.lang.String)
     */
    @Override
    public void publish(String topic, String content) throws IllegalArgumentException, NoSuchOverlayException, IncompatibleTypeException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!isValidTopicName(topic))
            throw new IllegalArgumentException("topic '" + topic + "' can not use for topic name");
        if (content == null)
            throw new NullPointerException("content should not be null");
        if (content.isEmpty())
            throw new IllegalArgumentException("content should not be empty");
        if (userId == null)
            throw new IllegalStateException("UserId is not set");

        logger.debug("[{}] publish {}, {}", getUserId(), topic, content);

        // topic と Overlay を対応づける
        bindSubscribeOverlay(topic);

        // publish 通知
        monitor.putPublish(topic);

        // OFM 使用の判定
        boolean useofm = monitor.useOFM(topic);

        InetSocketAddress ofmaddress = monitor.getOFMAddress(topic);    // キャッシュから取得
        long publishid = publishIdCounter.incrementAndGet();

        // ALM 経由 publish と OFM 経由 publish を並走させるため、 OFM 経由 publish はここで行う
        // loopback 待ちは ALM 経由 publish の応答の後に行う
        PublishMessage ofmmsg = null;
        if (ofmaddress != null && useofm) {
            // OFM 経由の publish
            String ofmkey = getOFMKey(topic);
            ofmmsg = new PublishMessage(userId, topic, content, publishid, ofmaddress, PublishPath.OFM, ofmkey);

            // publish 元ピアIDを初期追加
            ofmmsg.addTransPath(new TransPathInfo(this.getMotherPeerId()));

            ofmmsg.setPublishTime(System.currentTimeMillis());

            logger.info("[{}] Publish message to Openflow. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]", 
                    getUserId(),
                    ofmmsg.getUserId(), ofmmsg.getPublishId(), ofmmsg.getPublishPath(),
                    ofmmsg.getPublishTime(), ofmmsg.getTopic(), ofmmsg.getContent());

            // subscriber に publish
            this.discoveryCall(ofmkey +" eq \"" + ofmkey + "\"", "onReceivePublish", ofmmsg);
        }

        // topic への ALM publish
        // ofmaddress が既知の場合は、この publish で subscriber に ofmaddress が知らされる
        PublishMessage topicmsg = new PublishMessage(userId, topic, content, publishid, ofmaddress, PublishPath.Overlay, topic);
        topicmsg.setPublishTime(System.currentTimeMillis());

        logger.info("[{}] Publish message to ALM topic. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]", 
                getUserId(),
                topicmsg.getUserId(), topicmsg.getPublishId(), topicmsg.getPublishPath(),
                topicmsg.getPublishTime(), topicmsg.getTopic(), topicmsg.getContent());

        // publish 元ピアIDを初期追加
        topicmsg.addTransPath(new TransPathInfo(this.getMotherPeerId()));

        long before_time = System.currentTimeMillis();
        FutureQueue<?> topicfq = this.discoveryCallAsync(topic +" eq \"" + topic + "\"", "onReceivePublish", topicmsg);
        // topic 宛て ALM publish の応答処理
        Set<SubscriberInfo> subs = new HashSet<>();
        for (RemoteValue<?> remote : topicfq) {
            long delay = System.currentTimeMillis() - before_time;
            if (remote.getException() == null) {
                if (remote.getValue() != null) {
                    try {
                        SubscriberInfo info = (SubscriberInfo) remote.getValue();
                        info.setDcDelay(delay);
                        subs.add(info);
                    } catch (ClassCastException e) {
                        logger.error("[{}] discoveryCall to onReceivePublish returns illegal class : {}", getUserId(), remote.getValue().getClass());
                    }
                } else {
                    logger.debug("[{}] discoveryCall to onReceivePublish returns null from {}", getUserId(), remote.getPeer());
                }
            } else {
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                        getUserId(), 
                        ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                logger.error(remote.getException().getMessage(), remote.getException());
            }
        }
        monitor.putSubscriberInfo(topic, subs);


        // OFM publish の loopback 待ち処理
        // タイムアウトした場合は OFM topic への ALM publish に fallback する
        if (ofmaddress != null && ofmmsg != null && useofm) {
            // loopback message を待機する
            if (ofmmsg.waitLoopbackMsg(recvLoopbackTimeout)) {
                monitor.setOFMAddress(topic, ofmaddress);   // Monitor が持っている OFM アドレス情報を更新
                // OFM publish に成功したらそのまま returnする
                // この return を忘れると Overlay 経由で二重発信される
                return;
            }
            // OFM publish に失敗した場合は OFM topic への ALM publish に fallback する
            logger.warn("Failed OFM publish. No loopback message received. Try ALM publish.");

            // OFM アドレスを無効化
            monitor.setOFMAddress(topic, null);     // Monitor が持っている OFM アドレス情報を無効化

            // OFM topic への ALM publish に fallback する
        }

        // OFM topic への ALM publish
        String ofmtopic = getOFMtopic(topic);
        PublishMessage ofmtopicmsg = new PublishMessage(userId, topic, content, publishid, ofmaddress, PublishPath.Overlay, ofmtopic);

        ofmtopicmsg.setPublishTime(System.currentTimeMillis());

        logger.info("[{}] Publish message to OFM topic. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]", 
                getUserId(),
                ofmtopicmsg.getUserId(), ofmtopicmsg.getPublishId(), ofmtopicmsg.getPublishPath(),
                ofmtopicmsg.getPublishTime(), ofmtopicmsg.getTopic(), ofmtopicmsg.getContent());

        // publish 元ピアIDを初期追加
        ofmtopicmsg.addTransPath(new TransPathInfo(this.getMotherPeerId()));

        before_time = System.currentTimeMillis();
        FutureQueue<?> ofmtopicfq = this.discoveryCallAsync(ofmtopic +" eq \"" + ofmtopic + "\"", "onReceivePublish", ofmtopicmsg);
        subs = new HashSet<>();
        for (RemoteValue<?> remote : ofmtopicfq) {
            long delay = System.currentTimeMillis() - before_time;
            if (remote.getException() == null) {
                if (remote.getValue() != null) {
                    try {
                        SubscriberInfo info = (SubscriberInfo) remote.getValue();
                        info.setDcDelay(delay);
                        subs.add(info);
                    } catch (ClassCastException e) {
                        logger.error("[{}] discoveryCall to onReceivePublish returns illegal class : {}", getUserId(), remote.getValue().getClass());
                    }
                } else {
                    logger.debug("[{}] discoveryCall to onReceivePublish returns null from {}", getUserId(), remote.getPeer());
                }
            } else {
                StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                        getUserId(), 
                        ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                logger.error(remote.getException().getMessage(), remote.getException());
            }
        }

        // OFM topic への ALM publish で OFM address が得られた場合は monitor を介して cache に格納する
        boolean has_ofmaddress = false;
        for (SubscriberInfo info : subs) {
            if (info.getOfmAddress() != null) {
                monitor.setOFMAddress(topic, info.getOfmAddress());
                has_ofmaddress = true;
                break;
            }
        }

        // OFM topic で subscribe している subscriber の OFM address cache が expire した場合
        // （expire time 以上にOFM 経由でメッセージが受信されなかった場合）は、 OFM topic への publish の
        // 返り値に OFM アドレスが含まれない。
        // 1) publisher は OFM アドレスを知らない publisher のみ
        // 2) 全 subscriber の OFM address cache が expire していた場合
        // この条件時には publisher が何度 publish しても OFM アドレスを得られない
        // この場所で has_ofmaddress == false の場合がこの状態にあたるため、 OFGate から topic に対応する
        // OFM アドレスを得る
        if (!subs.isEmpty() && !has_ofmaddress) {
            ofmaddress = queryOFMAddress(topic);
            if (ofmaddress != null) {
                monitor.setOFMAddress(topic, ofmaddress);
            } else {
                // OFM topic に subscriber がいるにもかかわらず OFM アドレスが得られない場合
                // OFMGate が単独再起動した？
                logger.error("Something wrong. There are some subscribers, but no OFM address.");
            }
        }
    }


    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf#onReceivePublish(jp.piax.ofm.pubsub.common.PublishMessage)
     */
    @Override
    public SubscriberInfo onReceivePublish(PublishMessage msg) {
        // 受信時情報を追加
        long timestamp = System.currentTimeMillis();
        long deliver_delay = timestamp - msg.getPublishTime();
        counter.addReceiveInfo(timestamp, msg.getTopic(), deliver_delay, msg.getPublishPath());

        logger.info("[{}] Received a message. srcuser:[{}], via:[{}], msgid:[{}], timestamp[{}], topic:[{}], content:[{}], delay(ms):[{}]", 
                getUserId(),
                msg.getUserId(), msg.getPublishPath(), msg.getPublishId(),
                msg.getPublishTime(), msg.getTopic(), msg.getContent(), deliver_delay);

        // msg.getOfmAddress() != null であっても、そのアドレスを知ることによって OFM topic に移行してはいけない
        // なぜなら、 OFM のメンバーに未登録な場合があるため、移行するとメッセージが受信できなくなる
        // OFM からメッセージが受信できた場合（OFM のメンバーへの登録が確認できた場合）に初めて OFM topic に移行させる

        // OFM 経由で受信していた場合
        if (msg.getOfmAddress() != null && msg.getPublishPath() == PublishPath.OFM) {
            // OFM アドレスキャッシュを更新する
            monitor.setOFMAddress(msg.getTopic(), msg.getOfmAddress());
            if (subscribedTopic.get(msg.getTopic()) != ListenState.OFM) {
                // OFM topic に移行する
                migrateOFMtopic(msg.getTopic());
            }
        }

        // 転送経路をコンソールに出力
        if (msg.getTransPath() != null) {
            long timeTemp = -1;
            Date Time = new Date(msg.getPublishTime());
            System.out.println("=ROOT=\n"+Time.toLocaleString()+"\n==配送開始==");
            for (TransPathInfo pi : msg.getTransPath()) {
                if (timeTemp != -1){
                    System.out.println("<"+(pi.getTime() - timeTemp)+" ms>");
                }else{
                    timeTemp = msg.getPublishTime();
                }
                System.out.println(pi);
                timeTemp = pi.getTime();
            }
            System.out.println("所要時間: " + (timeTemp - msg.getPublishTime()) +" ms");
            System.out.println("==配送終了==\n=ROOTend=");
            System.out.println();
        }

        boolean duplicated = duplicatedChecker.checkDuplicated(msg);
        if (duplicated) {
            logger.debug("[{}] Message is duplicated. srcuser:[{}], msgid:[{}]", getUserId(), msg.getUserId(), msg.getPublishId());
        }
        // 新規メッセージであれば listener に渡す
        if (listener != null && !duplicated) {
            logger.debug("[{}] Notify message to listener. srcuser:[{}], msgid:[{}]", getUserId(), msg.getUserId(), msg.getPublishId());
            listener.onReceive(msg.getUserId(), msg.getTopic(), msg.getContent());
        }
        return getSubscriberInfo(msg.getTopic());
    }

    /**
     * subscribe のメッセージ受け取りを Overlay 状態から OFM 状態に移行する
     * @param topic 移行させる topic 名
     */
    private void migrateOFMtopic(final String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        // ConcurrentHashMap の CAS 操作で排他制御
        if (subscribedTopic.replace(topic, ListenState.Overlay, ListenState.OFMMigrating)) {
            long delay = this.migrationDelayBase + (long) (this.migrationDelayAdditionalWidth * rndsrc.nextDouble());
            logger.info("[{}] Topic [{}] will migrate to OFM after {} (ms)", getUserId(), topic, delay);
            scheduledExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                   try {
                       String ofmtopic = getOFMtopic(topic);
                       YosPubSubAgent.this.setAttrib(ofmtopic, ofmtopic, true);
                       logger.debug("[{}] setAttrib {}", ofmtopic);
                       if (topic.equals(YosPubSubAgent.this.getAttribValue(topic))) {
                           YosPubSubAgent.this.removeAttrib(topic);
                           logger.debug("[{}] removeAttrib {}", topic);
                       }
                       subscribedTopic.replace(topic, ListenState.OFMMigrating, ListenState.OFM);
                       logger.info("[{}] Topic [{}] migrated to OFM", getUserId(), topic);
                   } catch (Exception e) {
                       logger.error(e.getMessage(), e);
                   }
                }
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 受け入れ可能なトピック名か判定する
     * @param topic トピック名
     * @return true 受け入れ可能 false 受け入れ不可
     */
    private boolean isValidTopicName(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        return !topic.startsWith(OFMPubSubOverlay.OFM_KEY_PREFIX);
    }

    /**
     * topic 名から OpenFlow Multicast 用のキー名を得る。
     * discoveryCall から OpenFlow Multicast を用いるには、このメソッドで得られるキー名を用いる
     * @param topic 
     * @return OpenFlow Multicast 用のキー名
     */
    private String getOFMKey(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        return OFMPubSubOverlay.OFM_KEY_PREFIX + topic;
    }

    /**
     * topic 名から OFM topic 名を得る。
     *
     * @param topic 
     * @return OpenFlow Multicast 用のキー名
     */
    private String getOFMtopic(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        return OFM_TOPIC_PREFIX + topic;
    }

    /**
     * topic に対応する PIAX の属性を登録する
     * @param topic 
     */
    private void declaredAttribute(String topic) {
        String ofmkey = getOFMKey(topic);
        String ofmtopic = getOFMtopic(topic);
        _declaredAttributes(new String[]{topic, ofmkey, ofmtopic});
    }

    /**
     * topic にオーバレイを対応づける
     * @param topic 
     */
    private void bindSubscribeOverlay(String topic) {
        String ofmkey = getOFMKey(topic);
        String ofmtopic = getOFMtopic(topic);
        _bindSubscribeOverlays(new String[]{topic, ofmkey, ofmtopic});
    }
}
