package jp.piax.ofm.pubsub.piax.agent;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.common.SetupOFMResult;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.common.PublishMessage.PublishPath;
import jp.piax.ofm.pubsub.common.TransPathInfo;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.misc.SubscriberCounter;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.SimpleMonitor;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;

import org.piax.common.PeerId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubAgent extends AbstructPubSubAgent implements PubSubAgentIf {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(PubSubAgent.class);

    /** OFM 判定周期兼 Subscriber 統計情報収集周期 (ms) */
    private long pollingPeriod = PubSubAgentConfigValues.SubscriberPollingPeriod;
    /** OpenFlow Multicast 時のメッセージ loopback 待ちタイムアウト (ms) */
    private long recvLoopbackTimeout = PubSubAgentConfigValues.RecvLoopbackTimeout;

    private volatile PubSubMonitor monitor = new SimpleMonitor();
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
                                        if (result != null && result.isFullySucceed() && result.getOfmAddress() != null) {
                                            monitor.setOFMAddress(topic, result.getOfmAddress());
                                            logger.info("[{}] setupOFM succeeded for topic:[{}]. OFM address: {}", getUserId(), topic, result.getOfmAddress());
                                        } else {
                                            if (result != null && !result.isFullySucceed() && result.getOfmAddress() != null) {
                                                logger.warn("[{}] setupOFM failed for topic:[{}]. Maybe the topic is already registered but subscribers does not match registered members", getUserId(), topic);
                                                if (logger.isTraceEnabled()) {
                                                    String req = IPMACPair.convertToOFMAddrString(subscribers);
                                                    String regged = IPMACPair.convertToOFMAddrString(result.getOfmMembers());
                                                    logger.trace("[{}] Members requested to setupOFM are [{}]", getUserId(), req);
                                                    logger.trace("[{}] Members already registered are [{}]", getUserId(), regged);
                                                }
                                            }
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
                this.monitor = PubSubAgentConfigValues.MonitorClazz.newInstance();
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("monitorclazz cannot new an instance : " + PubSubAgentConfigValues.MonitorClazz.getCanonicalName(), e);
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
        for (String topic : subscribedTopic) {
            if (subscribedTopicCounter.unsubscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバから削除要求
                if (notifyUnsubscribeToOFGate(topic) == null) {
                    monitor.setOFMAddress(topic, null); // OFM メンバから削除された結果、 OFM アドレスが無効化された
                }
            }
        }

        super.onDeparture();
    }


    @Override
    public void onArrival() {
        super.onArrival();

        OFMAddressCache ofmAddressCache = ((PubSubAgentHomeImpl) this.getHome()).getOFMAddressCache();
        monitor.attachOFMAddressCache(ofmAddressCache);

        // 移動後に subscribe カウンタを加算し、必要に応じて OFGate に通知
        // これを忘れると、エージェント移動時に OFM メンバに含まれないゾンビ subscriber が生じる
        for (String topic : subscribedTopic) {
            if (subscribedTopicCounter.subscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバに追加要求
                InetSocketAddress ofmaddress = notifySubscribeToOFGate(topic);
                monitor.setOFMAddress(topic, ofmaddress);
            }
        }

        // モニタリングスレッドを再始動する
        startMonitoringThread();

        // TODO listener 再設定（どうやって？）
    }


    @Override
    public void onDestruction() {
        // 終了前に subscribe カウンタを減じ、必要に応じて OFGate に通知
        // これを忘れると、エージェント破棄時に OFM メンバから外されないゾンビ subscriber が生じる
        for (String topic : subscribedTopic) {
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
    public synchronized void startMonitoringThread() {
        if (monitoringTask == null) {
            monitoringTask = scheduledExecutor.scheduleAtFixedRate(monitoring, pollingPeriod, pollingPeriod, TimeUnit.MILLISECONDS);
            logger.debug("[{}] Start monitoring thread. Monitoring subscribers at {} (ms) interval", getUserId(), pollingPeriod);
        }
    }

    /**
     * モニタリングスレッドを終了する
     * @throws InterruptedException 
     */
    public synchronized void stopMonitoringThread() throws InterruptedException {
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

        logger.debug("[{}] subscribe topic:[{}]", getUserId(), topic);

        // topic と Overlay を対応づける
        bindSubscribeOverlay(topic);

        synchronized (subscribedTopic) {
            if (subscribedTopic.contains(topic)) {
                logger.warn("[{}] Already subscribed", getUserId());
                return;
            }

            // エージェントにtopicを属性として持たせる
            this.setAttrib(topic, topic, true);
            logger.debug("[{}] setAttrib {}", getUserId(), topic);
            this.setAttrib(getOFMKey(topic), getOFMKey(topic), true);
            logger.debug("[{}] setAttrib {}", getUserId(), getOFMKey(topic));

            subscribedTopic.add(topic);

            // ピア上で最初に subscribe する場合は OFGate に通知
            if (subscribedTopicCounter.subscribeAndNeedUpdate(topic)) {
                // OFGate に OFM 受信アドレスを通知して OFM メンバに追加要求
                InetSocketAddress ofmaddress = notifySubscribeToOFGate(topic);
                monitor.setOFMAddress(topic, ofmaddress);
            }
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

        // topicに対応する属性が登録されていない場合は登録する
        declaredAttribute(topic);

        synchronized (subscribedTopic) {
            if (!subscribedTopic.contains(topic)) {
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

        InetSocketAddress ofmaddress = null;
        if (PubSubAgentConfigValues.UseOFMCacheBeforePublish) {
            ofmaddress = monitor.getOFMAddress(topic); // キャッシュから取得
        }
        // OFGate に OFM アドレスを問い合わせ
        if (ofmaddress == null) {
            ofmaddress = queryOFMAddress(topic);
            monitor.setOFMAddress(topic, ofmaddress);
        }

        // publish 通知
        monitor.putPublish(topic);

        // OFM 使用の判定
        boolean useofm = monitor.useOFM(topic);
        long publishid = publishIdCounter.incrementAndGet();

        if (ofmaddress != null && useofm) {
            // OFM 経由の publish
            String ofmkey = getOFMKey(topic);
            PublishMessage msg = new PublishMessage(userId, topic, content, publishid, ofmaddress, PublishPath.OFM, ofmkey);

            // publish 元ピアIDを初期追加
            msg.addTransPath(new TransPathInfo(this.getMotherPeerId()));

            msg.setPublishTime(System.currentTimeMillis());

            logger.info("[{}] Publish message to Openflow. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]", 
                    getUserId(),
                    msg.getUserId(), msg.getPublishId(), msg.getPublishPath(),
                    msg.getPublishTime(), msg.getTopic(), msg.getContent());

            // subscriber に publish
            this.discoveryCall(ofmkey +" eq \"" + ofmkey + "\"", "onReceivePublish", msg);

            // loopback message を待機する
            if (msg.waitLoopbackMsg(recvLoopbackTimeout)) {
                monitor.setOFMAddress(topic, ofmaddress);   // Monitor が持っている OFM アドレス情報を更新
                // OFM publish に成功したらそのまま returnする
                // この return を忘れると Overlay 経由で二重発信される
                return;
            }
            // OFM publish に失敗した場合
            logger.warn("[{}] Failed OFM publish. No loopback message received. Try ALM publish.", getUserId());

            // OFM アドレスを無効化
            monitor.setOFMAddress(topic, null);     // Monitor が持っている OFM アドレス情報を無効化

            // Overlay publish に fallback する
        }

        // Overlay 経由の publish
        PublishMessage msg = new PublishMessage(userId, topic, content, publishid, ofmaddress, PublishPath.Overlay, topic);

        msg.setPublishTime(System.currentTimeMillis());

        logger.info("[{}] Publish message to ALM topic. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]", 
                getUserId(),
                msg.getUserId(), msg.getPublishId(), msg.getPublishPath(),
                msg.getPublishTime(), msg.getTopic(), msg.getContent());

        // publish 元ピアIDを初期追加
        msg.addTransPath(new TransPathInfo(this.getMotherPeerId()));

        long before_time = System.currentTimeMillis();
        FutureQueue<?> fq = this.discoveryCallAsync(topic +" eq \"" + topic + "\"", "onReceivePublish", msg);
        Set<SubscriberInfo> subs = new HashSet<>();
        for (RemoteValue<?> remote : fq) {
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

        // OFM 経由で受信していた場合は OFM アドレスキャッシュを更新する
        if (msg.getOfmAddress() != null && msg.getPublishPath() == PublishPath.OFM) {
            monitor.setOFMAddress(msg.getTopic(), msg.getOfmAddress());
        }

        // 転送経路をコンソールに出力
        if (msg.getTransPath() != null) {
            long timeTemp = -1;
            Date Time = new Date(msg.getPublishTime());
            System.out.println("\n配送方式："+msg.getPublishPath());
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
            System.out.println("TimeStamp: "+ timestamp);
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
     * 受け入れ可能なトピック名か判定する
     * @param topic トピック名
     * @return true 受け入れ可能 false 受け入れ不可
     */
    public boolean isValidTopicName(String topic) {
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
    public String getOFMKey(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        return OFMPubSubOverlay.OFM_KEY_PREFIX + topic;
    }

    /**
     * topic に対応する PIAX の属性を登録する
     * @param topic 
     */
    public void declaredAttribute(String topic) {
        String ofmkey = getOFMKey(topic);
        _declaredAttributes(new String[]{topic, ofmkey});
    }

    /**
     * topic にオーバレイを対応づける
     * @param topic 
     */
    public void bindSubscribeOverlay(String topic) {
        String ofmkey = getOFMKey(topic);
        _bindSubscribeOverlays(new String[]{topic, ofmkey});
    }
    
    //shikata
    
    
    public InetSocketAddress notifySubscribeToOFGate(String topic){
        return notifySubscribeToOFGate(topic);
    }
    
    public InetSocketAddress notifyUnsubscribeToOFGate(String topic){
        return notifyUnsubscribeToOFGate(topic);
    }
    
    public PeerId getAgentMotherPeerId() {
        return this.getMotherPeerId();
    }
    
    public long getRecvLoopbackTimeout(){
        return recvLoopbackTimeout;
    }

}
