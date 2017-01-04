/*
 * PeerMqDeliveryToken.java - An implementation of delivery token.
 * 
 * Copyright (c) 2016 PIAX development team
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piqt.peer;

import java.net.InetSocketAddress;

import org.piax.common.Destination;
import org.piax.common.Endpoint;
import org.piax.common.subspace.KeyRange;
import org.piax.common.subspace.LowerUpper;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.RemoteValue;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.TransOptions.ResponseType;
import org.piax.gtrans.TransOptions.RetransMode;
import org.piax.gtrans.ov.Overlay;
import org.piax.util.KeyComparator;
import org.piqt.MqActionListener;
import org.piqt.MqCallback;
import org.piqt.MqDeliveryToken;
import org.piqt.MqException;
import org.piqt.MqMessage;
import org.piqt.MqTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.TransPathInfo;
import jp.piax.ofm.pubsub.common.PublishMessage.PublishPath;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgent;

public class PeerMqDeliveryToken implements MqDeliveryToken {
    private static final Logger logger = LoggerFactory
            .getLogger(PeerMqDeliveryToken.class);
    MqMessage m;
    // Overlay<KeyRange<LATKey>, LATKey> o;
    Overlay<Destination, LATKey> o;
    FutureQueue<?>[] qs;
    boolean isComplete = false;
    MqActionListener aListener = null;
    Object userContext = null;
    boolean isWaiting = false;
    MqCallback c = null;
    int seqNo = 0;
    public static int ACK_INTERVAL = -1;
    public static boolean USE_DELEGATE = true;

    TopicDelegator[] delegators;

    /** shikata for agent support **/
    // PubSubAgent agent;

    public PeerMqDeliveryToken(Overlay<Destination, LATKey> overlay,
            MqMessage message, MqCallback callback, int seqNo) {
        this.m = message;
        this.o = overlay;
        this.c = callback;
        this.seqNo = seqNo;
    }

    public TopicDelegator[] findDelegators(PeerMqEngine engine, String[] topics,
            int qos) throws MqException {
        FutureQueue<?>[] qs = new FutureQueue<?>[topics.length];
        TopicDelegator[] ds = new TopicDelegator[topics.length];
        try {
            for (int i = 0; i < topics.length; i++) {
                LATopic lat = new LATopic(topics[i]);
                if (engine.getClusterId() == null) {
                    lat = LATopic.clusterMax(lat);
                } else {
                    lat.setClusterId(engine.getClusterId());
                }
                @SuppressWarnings({ "unchecked", "rawtypes" })
                KeyRange<?> range = new KeyRange(
                        KeyComparator.getMinusInfinity(LATKey.class), false,
                        new LATKey(lat), true);
                // find the nearest engine.
                LowerUpper dst = new LowerUpper(range, false, 1);
                qs[i] = o.request(dst, (Object) new DelegatorCommand("find"),
                        new TransOptions(ResponseType.DIRECT, qos == 0
                                ? RetransMode.NONE : RetransMode.FAST));
            }
            FutureQueue<?>[] empties = new FutureQueue<?>[topics.length];
            for (int i = 0; i < qs.length; i++) {
                if (qs[i] != null) {
                    if (qs[i].isEmpty()) { // skip
                        logger.debug("empty queue for {}", topics[i]);
                        empties[i] = qs[i];
                        continue;
                    }
                    for (RemoteValue<?> rv : qs[i]) {
                        Endpoint e = (Endpoint) rv.getValue();
                        ds[i] = new TopicDelegator(e, topics[i]);
                        logger.debug("delegator for {} : {}", topics[i],
                                rv.getValue());
                    }
                } else {
                    logger.debug("response for {} was null.", topics[i]);
                }
            }
            // empties
            for (int i = 0; i < empties.length; i++) {
                if (empties[i] != null) {
                    for (RemoteValue<?> rv : empties[i]) {
                        Endpoint e = (Endpoint) rv.getValue();
                        ds[i] = new TopicDelegator(e, topics[i]);
                        logger.debug("delegator for {} : {}", topics[i],
                                rv.getValue());
                    }
                } else {
                    logger.debug("response for {} was null.", topics[i]);
                }
            }
        } catch (Exception e) {
            throw new MqException(e);
        }
        return ds;
    }

    boolean delegationCompleted() {
        for (TopicDelegator d : delegators) {
            if (d != null) {
                if (!d.succeeded) {
                    logger.debug("delegationCompleted: not finished: {}",
                            d.topic);
                    return false;
                }
            }
        }
        logger.debug("delegationCompleted: completed {}", m.getTopic());
        return true;
    }

    public void resetDelegators(TopicDelegator[] delegators) {
        for (TopicDelegator d : delegators) {
            if (d != null) {
                d.succeeded = false;
            }
        }
    }

    public boolean delegationSucceeded(String topic) {
        for (TopicDelegator d : delegators) {
            if (d != null) {
                logger.debug(
                        "delegationSucceeded: searching for {}, matching on {}",
                        topic, d.topic);
                if (d.topic.equals(topic)) {
                    logger.debug("delegationSucceeded: succeeded: {}", d.topic);
                    d.succeeded = true;
                }
            }
        }
        if (delegationCompleted()) {
            if (aListener != null) {
                aListener.onSuccess(this);
            }
            synchronized (this) {
                if (isWaiting) {
                    notify();
                }
            }
            if (c != null) {
                c.deliveryComplete(this);
            }
            m = null;
            isComplete = true;
            return true;
        }
        return false;
    }

    public void startDelivery(PeerMqEngine engine) throws MqException {
        if (USE_DELEGATE) {
            startDeliveryDelegate(engine);
        } else {
            startDeliveryEach(engine);
        }
    }

    public void startDeliveryDelegate(PeerMqEngine engine) throws MqException {
        String topic = m.getTopic();
        String[] pStrs = new MqTopic(topic).getPublisherKeyStrings();

        /** shikata **/
        String content = m.getContent();
        PubSubAgent agent = engine.getPubSubAgent();

        if (pStrs == null)
            throw new NullPointerException("topic should not be null");
        if (!engine.isValidTopicName(topic))
            throw new IllegalArgumentException(
                    "topic '" + topic + "' can not use for topic name");
        if (content == null)
            throw new NullPointerException("content should not be null");
        if (content.isEmpty())
            throw new IllegalArgumentException("content should not be empty");
        if (engine.getUserId() == null)
            throw new IllegalStateException("UserId is not set");

        logger.debug("[{}] publish {}, {}", engine.getUserId(), pStrs, content);

        // topic と Overlay を対応づける
        for (String pTopic : pStrs) {
            
            engine.bindSubscribeOverlay(pTopic);

            PubSubMonitor monitor = engine.getPubSubMonitor();
            InetSocketAddress ofmaddress = null;
            if (PubSubAgentConfigValues.UseOFMCacheBeforePublish) {
                ofmaddress = monitor.getOFMAddress(pTopic); // キャッシュから取得
            }
            // OFGate に OFM アドレスを問い合わせ
            if (ofmaddress == null) {
                ofmaddress = agent.queryOFMAddress(pTopic);
                monitor.setOFMAddress(pTopic, ofmaddress);
            }

            // publish 通知
            monitor.putPublish(pTopic);

            // OFM 使用の判定
            boolean useofm = monitor.useOFM(pTopic);
            long publishid = agent.publishIdCounter.incrementAndGet();

            /** shikata **/

            if (ofmaddress != null && useofm) {
                // OFM 経由の publish
                String ofmkey = engine.getOFMKey(pTopic);
                PublishMessage msg = new PublishMessage(engine.getUserId(),
                        pTopic, content, publishid, ofmaddress, PublishPath.OFM,
                        ofmkey);

                // publish 元ピアIDを初期追加
                msg.addTransPath(
                        new TransPathInfo(agent.getAgentMotherPeerId()));

                msg.setPublishTime(System.currentTimeMillis());

                logger.info(
                        "[{}] Publish message to Openflow. srcuser:[{}], msgid:[{}], via:[{}], timestamp[{}], topic:[{}], content:[{}]",
                        engine.getUserId(), msg.getUserId(), msg.getPublishId(),
                        msg.getPublishPath(), msg.getPublishTime(),
                        msg.getTopic(), msg.getContent());

                // subscriber に publish
                agent.discoveryCall(ofmkey + " eq \"" + ofmkey + "\"",
                        "onReceivePublish", msg);
                
                

                // loopback message を待機する
                if (msg.waitLoopbackMsg(agent.getRecvLoopbackTimeout())) {
                    monitor.setOFMAddress(pTopic, ofmaddress); // Monitor が持っている
                                                              // OFM アドレス情報を更新
                    // OFM publish に成功したらそのまま returnする
                    // この return を忘れると Overlay 経由で二重発信される
                    return;
                }
                // OFM publish に失敗した場合
                logger.warn(
                        "[{}] Failed OFM publish. No loopback message received. Try ALM publish.",
                        engine.getUserId());

                // OFM アドレスを無効化
                monitor.setOFMAddress(pTopic, null); // Monitor が持っている OFM
                                                    // アドレス情報を無効化

                // Overlay publish に fallback する
            }
        }
        
        // Overlay 経由の publish
        int qos = m.getQos();
        /* delegators for the topic */
        delegators = engine.getDelegators(topic);
        if (delegators == null) {
            delegators = findDelegators(engine, pStrs, qos);
            engine.foundDelegators(m.getTopic(), delegators);
        } else {
            resetDelegators(delegators);
        }
        logger.debug("delegate: m={}", m);
        
        for (TopicDelegator d : delegators) {
            if (d != null) {
                logger.debug("delegate: endpoint={}, topic={}, m={}",
                        d.endpoint, d.topic, m);
                ;
                engine.delegate(this, d.endpoint, d.topic, m);
            }
        }

        /*
         * if (aListener != null) { aListener.onSuccess(this); }
         * synchronized(this) { if (isWaiting) { notify(); } } if (c != null) {
         * c.deliveryComplete(this); }
         */

        /** shikata **/

        /** shikata **/
    }

    public void startDeliveryEach(PeerMqEngine engine) throws MqException {
        try {
            String topic = m.getTopic();
            String[] pStrs = new MqTopic(topic).getPublisherKeyStrings();

            RetransMode mode;
            ResponseType type;
            TransOptions opts;
            switch (m.getQos()) {
            case 0:
                type = ResponseType.NO_RESPONSE;
                if (ACK_INTERVAL < 0) {
                    mode = RetransMode.NONE;
                } else {
                    mode = (seqNo % ACK_INTERVAL == 0) ? RetransMode.NONE_ACK
                            : RetransMode.NONE;
                }
                opts = new TransOptions(type, mode);
                break;
            default: // 1, 2
                type = ResponseType.AGGREGATE;
                mode = RetransMode.FAST;
                opts = new TransOptions(PeerMqEngine.DELIVERY_TIMEOUT, type,
                        mode);
                break;
            }

            qs = new FutureQueue<?>[pStrs.length];
            for (int i = 0; i < pStrs.length; i++) {
                qs[i] = o.request(
                        new KeyRange<LATKey>(
                                new LATKey(LATopic.topicMin(pStrs[i])),
                                new LATKey(LATopic.topicMax(pStrs[i]))),
                        (Object) m, opts);
            }
        } catch (Exception e) {
            if (aListener != null) {
                aListener.onFailure(this, e);
            }
            throw new MqException(e);
        }
        // ClusterId closest = null;
        for (FutureQueue<?> q : qs) {
            for (RemoteValue<?> rv : q) {
                /*
                 * response is ClusterId ClusterId cid =
                 * (ClusterId)rv.getValue(); if (closest == null ||
                 * closest.distance(cid) <
                 * closest.distance(engine.getClusterId())) { closest = cid; }
                 */
                Throwable t = null;
                if ((t = rv.getException()) != null) {
                    if (aListener != null) {
                        aListener.onFailure(this, t);
                    }
                }
            }
        }
        if (aListener != null) {
            aListener.onSuccess(this);
        }
        synchronized (this) {
            if (isWaiting) {
                notify();
            }
        }
        if (c != null) {
            c.deliveryComplete(this);
        }
        m = null;
        isComplete = true;
    }

    @Override
    public void waitForCompletion() throws MqException {
        synchronized (this) {
            if (!isComplete) {
                try {
                    isWaiting = true;
                    wait();
                } catch (InterruptedException e) {
                    if (aListener != null) {
                        aListener.onFailure(this, e);
                    }
                    throw new MqException(e);
                } finally {
                    isWaiting = false;
                }
            }
        }
    }

    @Override
    public void waitForCompletion(long timeout) throws MqException {
        synchronized (this) {
            if (!isComplete) {
                try {
                    isWaiting = true;
                    wait(timeout);
                } catch (InterruptedException e) {
                    if (aListener != null) {
                        aListener.onFailure(this, e);
                    }
                    throw new MqException(e);
                } finally {
                    isWaiting = false;
                }
            }
        }
    }

    @Override
    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public MqException getException() {
        return null;
    }

    @Override
    public void setActionCallback(MqActionListener listener) {
        aListener = listener;
    }

    @Override
    public MqActionListener getActionCallback() {
        return aListener;
    }

    @Override
    public String[] getTopics() {
        return new String[] { m.getTopic() };
    }

    @Override
    public void setUserContext(Object userContext) {
        this.userContext = userContext;
    }

    @Override
    public Object getUserContext() {
        return this.userContext;
    }

    @Override
    public int getMessageId() {
        // XXX Message id has no meaning
        return 0;
    }

    @Override
    public MqMessage getMessage() {
        return m;
    }

}
