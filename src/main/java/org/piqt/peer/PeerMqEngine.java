/*
 * PeerMqEngine.java - An implementation of pub/sub engine.
 * 
 * Copyright (c) 2016 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piqt.peer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AlgorithmParameterGenerator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piqt.MqCallback;
import org.piqt.MqDeliveryToken;
import org.piqt.MqEngine;
import org.piqt.MqException;
import org.piqt.MqMessage;
import org.piqt.MqTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** for AgentAPI **/
import jp.piax.ofm.pubsub.UserPubSubImpl;
import jp.piax.ofm.pubsub.common.PubSubException;
import jp.piax.ofm.pubsub.piax.agent.AbstructPubSubAgent;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgent;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf;

import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.PubSubManagerImplConfig;
import org.piax.agent.AgentException;
import org.piax.agent.AgentHome;
import org.piax.agent.AgentId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.ov.NoSuchOverlayException;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
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
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.SimpleMonitor;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;

import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.RemoteValue;


/** shikata **/




public class PeerMqEngine implements MqEngine,
        OverlayListener<Destination, LATKey> {
    private static final Logger logger = LoggerFactory
            .getLogger(PeerMqEngine.class);
    Peer peer;
    PeerId pid;
    PeerLocator seed;
    Overlay<Destination, LATKey> o;
    MqCallback callback;
    Delegator<Endpoint> d;
    String host;
    int port;

    ClusterId clusterId = null;

    List<MqTopic> subscribes; // subscribed topics;
    List<LATKey> joinedKeys; // joined keys;

    public static int DELIVERY_TIMEOUT = 3000;
    int seqNo;
    
    //shikata
    private String userId = null;
    private AgentId userAgentId = null;
    private AgentHome home = null;
    //private PubSubAgentIf stub = null;
    private PubSubAgent agent = null;
    
    private static final long serialVersionUID = 1L;
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
            logger.debug("[{}] Start monitoring task", agent.getUserId());
            for (String topic : monitor.getMonitoringTopics()) {
                logger.info("[{}] Start monitoring for topic:[{}]", agent.getUserId(), topic);
                try {
                    // subscriber の情報を収集
                    FutureQueue<?> fq = agent.discoveryCallAsync(topic +" eq \"" + topic + "\"", "getSubscriberInfo", topic);

                    Set<SubscriberInfo> info = new HashSet<SubscriberInfo>();
                    for (RemoteValue<?> remote : fq) {
                        if (remote.getException() == null) {
                            if (remote.getValue() != null) {
                                info.add((SubscriberInfo) remote.getValue());
                            } else {
                                logger.error("[{}] discoveryCall getSubscriberInfo retuens null from {}", agent.getUserId(), remote.getPeer());
                            }
                        } else {
                            StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                            logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                                    agent.getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                            logger.error(remote.getException().getMessage(), remote.getException());
                        }
                    }
                    logger.info("[{}] get subscriber information for topic:[{}] from {} subscribers", agent.getUserId(), topic, info.size());
                    monitor.putSubscriberInfo(topic, info);

                    // OFM 網構築判定
                    if (monitor.setupOFM(topic)) {
                        Set<IPMACPair> subscribers = monitor.getSubscriberAddress(topic);
                        if (subscribers != null && !subscribers.isEmpty()) {
                            logger.info("[{}] try setupOFM for topic:[{}]", agent.getUserId(), topic);
                            // OFM 網構築
                            fq = agent.discoveryCallAsync(CommonValues.OFGATE_ATTRIB +" eq \"" + CommonValues.OFGATE_VALUE + "\"", "setupOFM", topic, subscribers);
                            for (RemoteValue<?> remote : fq) {
                                if (remote.getException() == null) {
                                    if (remote.getValue() != null) {
                                        SetupOFMResult result = (SetupOFMResult) remote.getValue();
                                        if (result != null && result.isFullySucceed() && result.getOfmAddress() != null) {
                                            monitor.setOFMAddress(topic, result.getOfmAddress());
                                            logger.info("[{}] setupOFM succeeded for topic:[{}]. OFM address: {}", agent.getUserId(), topic, result.getOfmAddress());
                                        } else {
                                            if (result != null && !result.isFullySucceed() && result.getOfmAddress() != null) {
                                                logger.warn("[{}] setupOFM failed for topic:[{}]. Maybe the topic is already registered but subscribers does not match registered members", agent.getUserId(), topic);
                                                if (logger.isTraceEnabled()) {
                                                    String req = IPMACPair.convertToOFMAddrString(subscribers);
                                                    String regged = IPMACPair.convertToOFMAddrString(result.getOfmMembers());
                                                    logger.trace("[{}] Members requested to setupOFM are [{}]", agent.getUserId(), req);
                                                    logger.trace("[{}] Members already registered are [{}]", agent.getUserId(), regged);
                                                }
                                            }
                                        }
                                    } else {
                                        logger.debug("[{}] setupOFM for topic:[{}] returns null from {}", agent.getUserId(), topic, remote.getPeer());
                                    }
                                } else {
                                    StackTraceElement[] ste = Thread.currentThread().getStackTrace();
                                    logger.error("[{}] RemoteException at {} line:{} in remote {}", 
                                            agent.getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
                                    logger.error(remote.getException().getMessage(), remote.getException());
                                }
                                break;
                            }
                        } else {
                            logger.debug("[{}] no subscribers of topic:[{}]", agent.getUserId(), topic);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    logger.debug("[{}] Finish monitoring for topic [{}]", agent.getUserId(), topic);
                }
            }
            logger.debug("[{}] Finish monitoring task", agent.getUserId());
        }
    };

    public void creatPubSubAgent(PubSubAgentHomeImpl home, PubSubManagerImplConfig config, AgentId useragentid, String userid)throws AgentException{
        useragentid = home.createAgent(config.getPubsubAgent(), userid);
        agent = home.getStub(null, useragentid);
        agent.setUserId(userid);
    }
    
   public PubSubAgent getPubSubAgent(){
       return agent;
   }
   
   public String getUserId(){
       return userId;
   }

    public PeerMqEngine(Overlay<Destination, LATKey> overlay)
            throws MqException {
        subscribes = new ArrayList<MqTopic>();
        joinedKeys = new ArrayList<LATKey>();
        o = overlay;
        Transport<?> t = o.getLowerTransport();
        peer = t.getPeer();
        pid = peer.getPeerId();
        try {
            d = new Delegator<Endpoint>(this);
        } catch (Exception e) {
            throw new MqException(e);
        }
        seqNo = 0;
        
        //agentAPI suport (shikata)
        this.home = home;
        //stub = home.getStub(null, userAgentId);
        this.userId = peer.getPeerId().toString();
    }

    public PeerMqEngine(String host, int port) throws MqException {
        subscribes = new ArrayList<MqTopic>();
        joinedKeys = new ArrayList<LATKey>();
        peer = Peer.getInstance(pid = PeerId.newId());
        try {
            o = new Suzaku<Destination, LATKey>(
                    peer.newBaseChannelTransport(new UdpLocator(
                            new InetSocketAddress(host, port))));
            d = new Delegator<Endpoint>(this);
        } catch (Exception e) {
            throw new MqException(e);
        }
    }

    ConcurrentHashMap<Integer, PeerMqDeliveryToken> tokens = new ConcurrentHashMap<Integer, PeerMqDeliveryToken>();

    ConcurrentHashMap<String, TopicDelegator[]> delegateCache = new ConcurrentHashMap<String, TopicDelegator[]>();

    
    /** Agent Support monitor**/
    public void onCreation() {
        agent.onCreation();

    }

    public void onDeparture() {
        // 移動前にモニタリングスレッドを終了させる
        agent.onDeparture();
       
    }


    public void onArrival() {
        agent.onArrival();
        
    }


    public void onDestruction() {
        agent.onDestruction();
        
    }

    /**
     * モニタリングスレッドを開始する
     */
    private synchronized void startMonitoringThread() {
        agent.startMonitoringThread();
    }

    /**
     * モニタリングスレッドを終了する
     * @throws InterruptedException 
     */
    private synchronized void stopMonitoringThread() throws InterruptedException {
        agent.stopMonitoringThread();
    }
    /*** AgentAPI  shikata ***/
    
    
    public void delegate(PeerMqDeliveryToken token, Endpoint e, String topic,
            MqMessage message) {
        DelegatorIf dif = d.getStub(e);
        tokens.put(token.seqNo, token);
        dif.delegate(o.getLowerTransport().getEndpoint(), token.seqNo, topic,
                message);
    }

    public void foundDelegators(String topic, TopicDelegator[] delegators) {
        delegateCache.put(topic, delegators);
    }

    public TopicDelegator[] getDelegators(String topic) {
        return delegateCache.get(topic);
    }

    public void delegationSucceeded(int tokenId, String topic) {
        PeerMqDeliveryToken token = tokens.get(tokenId);
        if (token == null) {
            logger.info("unregistered delivery succeeded: tokenId={}", tokenId);
        }
        if (token.delegationSucceeded(topic)) {
            tokens.remove(tokenId);
        }
    }

    public void setSeed(String host, int port) {
        seed = new UdpLocator(new InetSocketAddress(host, port));
    }

    public void setClusterId(String clusterId) {
        this.clusterId = new ClusterId(clusterId);
    }

    public ClusterId getClusterId() {
        return this.clusterId;
    }

    public List<LATKey> getJoinedKeys() {
        return joinedKeys;
    }

    synchronized void countUpSeqNo() {
        seqNo++;
    }

    synchronized int nextSeqNo() {
        return seqNo;
    }

    @Override
    public void connect() throws MqException {
        try {
            if (seed == null) {
                throw new MqException(MqException.REASON_SEED_NOT_AVAILABLE);
            }
            o.join(seed);
            o.setListener(this);
        } catch (Exception e) {
            throw new MqException(e);
        }
    }

    @Override
    public void disconnect() throws MqException {
        if (isConnected()) {
            try {
                o.leave();
            } catch (IOException e) {
                new MqException(e);
            }
        }
    }

    @Override
    public void subscribe(String topic) throws MqException, IllegalArgumentException, IncompatibleTypeException {
        // shikata
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!isValidTopicName(topic))
            throw new IllegalArgumentException("topic '" + topic + "' can not use for topic name");
        if (userId == null)
            throw new IllegalStateException("UserId is not set");
        
        logger.debug("[{}] subscribe topic:[{}]", agent.getUserId(), topic);
        // shikata
        
        MqTopic sTopic = new MqTopic(topic);
        subscribes.add(sTopic);
        String sOFMTopic = sTopic.getSubscriberKeyString();
        try {
            LATopic latk = new LATopic(sTopic.getSubscriberKeyString());
            if (clusterId != null) {
                latk.setClusterId(clusterId);
            }
            LATKey key = new LATKey(latk);
            boolean included = false;
            MqTopic stripped = new MqTopic(key.getKey().getTopic());
            for (LATKey jk : joinedKeys) {
                for (String pk : stripped.getPublisherKeyStrings()) {
                    // ex.
                    // newly joining key: 'sport/tennis/player1'
                    // joined keys: 'sport/tennis'
                    // -> no need to join (included)
                    if (pk.equals(jk.getKey().getTopic())) {
                        included = true;
                    }
                }
            }

            if (!included) {
                // newly joining key: 'sport/tennis'
                // joined key: 'sport/tennis/player1'
                // -> joined key becomes useless
                List<LATKey> uselessKeys = new ArrayList<LATKey>();
                for (LATKey jk : joinedKeys) {
                    MqTopic joined = new MqTopic(jk.getKey().getTopic());
                    for (String jpk : joined.getPublisherKeyStrings()) {
                        if (jpk.equals(key.getKey().getTopic())) {
                            uselessKeys.add(jk);
                        }
                    }
                }
                for (LATKey uKey : uselessKeys) {
                    o.removeKey(uKey);
                    joinedKeys.remove(uKey);
                }
                o.addKey(key);
                joinedKeys.add(key);
                
                // topic と Overlay を対応づける shikata
                bindSubscribeOverlay(sOFMTopic);
                Set<String> subscribedTopic = agent.subscribedTopic;
                synchronized (agent.subscribedTopic) {
                    if (subscribedTopic.contains(sOFMTopic)) {
                        logger.warn("[{}] Already subscribed", agent.getUserId());
                        return;
                    }

                    // エージェントにtopicを属性として持たせる
                    /* ALMのほうは必要ない
                    agent.setAttrib(topic, topic, true);
                    logger.debug("[{}] setAttrib {}", agent.getUserId(), topic);
                    */
                    agent.setAttrib(getOFMKey(sOFMTopic), getOFMKey(sOFMTopic), true);
                    logger.debug("[{}] setAttrib {}", agent.getUserId(), getOFMKey(sOFMTopic));

                    subscribedTopic.add(sOFMTopic);

                    // ピア上で最初に subscribe する場合は OFGate に通知
                    if (agent.subscribedTopicCounter.subscribeAndNeedUpdate(sOFMTopic)) {
                        // OFGate に OFM 受信アドレスを通知して OFM メンバに追加要求
                        InetSocketAddress ofmaddress = agent.notifySubscribeToOFGate(sOFMTopic);
                        monitor.setOFMAddress(sOFMTopic, ofmaddress);
                    }
                }
                //shikata
                
            }
        } catch (IOException e) {
            throw new MqException(e);
        }
    }

    @Override
    public void subscribe(String[] topics) throws MqException, IllegalArgumentException, IncompatibleTypeException {
        for (String topic : topics) {
            subscribe(topic);
        }
    }

    @Override
    public void setCallback(MqCallback callback) {
        this.callback = callback;
    }

    @Override
    public boolean isConnected() {
        return o.isJoined();
    }

    @Override
    public void fin() throws MqException {
        peer.fin();
    }

    @Override
    public void subscribe(String[] topics, int[] qos) throws MqException, IllegalArgumentException, IncompatibleTypeException {
        // XXX qos is ignored at this moment.
        subscribe(topics);
    }

    @Override
    public void unsubscribe(String topic) throws MqException {
        // shikata
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!isValidTopicName(topic))
            throw new IllegalArgumentException("topic '" + topic + "' can not use for topic name");
        if (userId == null)
            throw new IllegalStateException("UserId is not set");

        logger.info("[{}] unsubscribe topic:[{}]", agent.getUserId(), topic);

        String ofmkey = getOFMKey(topic);
        
        /*** shikata **/
        
        
        String sKeyStr = null;
        for (Iterator<MqTopic> it = subscribes.iterator(); it.hasNext();) {
            MqTopic t = it.next();
            sKeyStr = t.getSubscriberKeyString();
            if (t.getSpecified().equals(topic)) {
                it.remove();
            }
        }
        if (sKeyStr != null) {
            boolean anotherExists = false;
            for (MqTopic t : subscribes) {
                if (sKeyStr.equals(t.getSubscriberKeyString())) {
                    anotherExists = true;
                }
            }
            if (!anotherExists) {
                try {
                    LATKey sKey = new LATKey(new LATopic(sKeyStr));
                    joinedKeys.remove(sKey);
                    o.removeKey(sKey);
                    
                    
                    /*** shikata  **/
                    
                    // topicに対応する属性が登録されていない場合は登録する
                    declaredAttribute(sKeyStr);
                    Set<String> subscribedTopic = agent.subscribedTopic;
                    synchronized (subscribedTopic) {
                        if (!subscribedTopic.contains(sKeyStr)) {
                            return;
                        }
                     // エージェントが持っていた属性を削除する
                        // ALM側は必要ない
                        /*
                        if (topic.equals(this.getAttribValue(topic))) {
                            this.removeAttrib(topic);
                            logger.debug("[{}] removeAttrib {}", getUserId(), topic);
                        }
                        */
                        if (ofmkey.equals(agent.getAttribValue(ofmkey))) {
                            agent.removeAttrib(ofmkey);
                            logger.debug("[{}] removeAttrib {}", agent.getUserId(), ofmkey);
                        }

                        subscribedTopic.remove(topic);

                        // ピア上で最後に unsubscribe する場合は OFGate に通知
                        if (agent.subscribedTopicCounter.unsubscribeAndNeedUpdate(sKeyStr)) {
                            // OFGate に OFM 受信アドレスを通知して OFM メンバから削除要求
                            if (agent.notifyUnsubscribeToOFGate(sKeyStr) == null) {
                                monitor.setOFMAddress(sKeyStr, null); // OFM メンバから削除された結果、 OFM アドレスが無効化された
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new MqException(e);
                }
            }
        }
    }

    public Overlay<Destination, LATKey> getOverlay() {
        return o;
    }

    public void setOverlay(Overlay<Destination, LATKey> o) {
        this.o = o;
    }

    @Override
    public void unsubscribe(String[] topics) throws MqException {
        for (String topic : topics) {
            unsubscribe(topic);
        }
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, String content)
            throws MqException {
        MqMessage m = new MqMessage(topic);
        m.setPayload(payload);
        m.setQos(qos);
        m.setContent(content); // shikata for SAPS
        publish(m);
    }

    @Override
    public void onReceive(Overlay<Destination, LATKey> ov,
            OverlayReceivedMessage<LATKey> rmsg) {
        logger.debug("onReceive on Transport: trans={} rmsg={}", ov, rmsg);
        try {
            for (MqTopic t : subscribes) {
                MqMessage m = (MqMessage) rmsg.getMessage();
                if (t.matchesToTopic(m.getTopic())) {
                    callback.messageArrived(t, m);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public FutureQueue<?> onReceiveRequest(Overlay<Destination, LATKey> ov,
            OverlayReceivedMessage<LATKey> rmsg) {
        logger.debug("onReceiveRequest on Overlay: overlay={} rmsg={}", ov,
                rmsg);

        Object msg = rmsg.getMessage();
        if (msg instanceof DelegatorCommand) {
            logger.debug(o.getEndpoint() + ":" + getClusterId());
            return ov.singletonFutureQueue(o.getLowerTransport().getEndpoint());
        }

        List<String> matched = new ArrayList<String>();
        Throwable th = null;
        try {
            for (MqTopic t : subscribes) {
                MqMessage m = (MqMessage) rmsg.getMessage();
                if (t.matchesToTopic(m.getTopic())) {
                    matched.add(t.getSpecified());
                    callback.messageArrived(t, m);
                }
            }
        } catch (Exception e) {
            th = e;
        }
        return ov.singletonFutureQueue(getPeerId(), th);
    }

    @Override
    public MqDeliveryToken publishAsync(MqMessage m) throws MqException {
        countUpSeqNo();
        PeerMqDeliveryToken t = new PeerMqDeliveryToken(o, m, callback,
                nextSeqNo());
        t.startDelivery(this);
        return t;
    }

    @Override
    public void publish(MqMessage m) throws MqException {
        PeerMqDeliveryToken t = (PeerMqDeliveryToken) publishAsync(m);
        t.waitForCompletion();
    }

    public PeerId getPeerId() {
        return pid;
    }

    @Override
    public void onReceive(Transport<Destination> trans, ReceivedMessage rmsg) {
        logger.debug("onReceive on Transport: trans={} rmsg={}", trans, rmsg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.piax.mq.MqEngine#subscribedTo(java.lang.String)
     */
    @Override
    public boolean subscribedTo(String topic) {
        for (LATKey k : joinedKeys) {
            if (k.getKey().getTopic().equals(topic)) {
                return true;
            }
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.piax.mq.MqEngine#subscriptionMatchesTo(java.lang.String)
     */
    @Override
    public boolean subscriptionMatchesTo(String topic) {
        for (MqTopic t : subscribes) {
            try {
                if (t.matchesToTopic(topic)) {
                    return true;
                }
            } catch (MqException e) {
                return false;
            }
        }
        return false;
    }
    
    /**
     * shikata
     * for Agnet
     */
    public PubSubMonitor getPubSubMonitor(){
        return monitor;
    }
    
    /**
     * 受け入れ可能なトピック名か判定する
     * @param topic トピック名
     * @return true 受け入れ可能 false 受け入れ不可
     */
    public boolean isValidTopicName(String topic) {
        return agent.isValidTopicName(topic);
    }
    
    /**
     * topic 名から OpenFlow Multicast 用のキー名を得る。
     * discoveryCall から OpenFlow Multicast を用いるには、このメソッドで得られるキー名を用いる
     * @param topic 
     * @return OpenFlow Multicast 用のキー名
     */
    public String getOFMKey(String topic) {
        return agent.getOFMKey(topic);
    }

    /**
     * topic に対応する PIAX の属性を登録する
     * @param topic 
     */
    public void declaredAttribute(String topic) {
        agent.declaredAttribute(topic);
    }

    /**
     * topic にオーバレイを対応づける
     * @param topic 
     */
    public void bindSubscribeOverlay(String topic) {
        agent.bindSubscribeOverlay(topic);
    }
    
    /****  shikata **/

    public static void main(String args[]) {
        try {
            PeerMqEngine engine = new PeerMqEngine("localhost", 12367);
            engine.setCallback(new MqCallback() {
                @Override
                public void messageArrived(MqTopic subscribedTopic, MqMessage m)
                        throws Exception {
                    System.out.println("received:" + m + " on subscription:"
                            + subscribedTopic.getSpecified() + " for topic:"
                            + m.getTopic());
                }

                @Override
                public void deliveryComplete(MqDeliveryToken token) {
                    System.out.println("delivered:"
                            + token.getMessage().getTopic());
                }
            });
            engine.setSeed("localhost", 12367);
            // engine.setClusterId("cluster.test");
            engine.connect();
            
            engine.subscribe("sport/tennis/player1");
            System.out.println("joinedKeys=" + engine.getJoinedKeys());
            
            engine.publish("sport/tennis/player1", "hello1".getBytes(), 0);
            
            engine.subscribe("#");
            engine.subscribe("+/#");
            engine.subscribe("/+/#");
            engine.subscribe("sport/+");
            System.out.println("joinedKeys=" + engine.getJoinedKeys());
            System.out.println("sleeping 20 sec");
            Thread.sleep(20000);
            
            engine.publish("sport/tennis", "hello2".getBytes(), 0);
            engine.publish("/sport/tennis", "hello3".getBytes(), 1);

            engine.disconnect();
            engine.fin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void publish(String topic, byte[] payload, int qos)
            throws MqException {
        // TODO Auto-generated method stub
        
    }
}
