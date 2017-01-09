package jp.piax.ofm.pubsub.piax.agent;

import java.util.HashSet;
import java.util.Set;
import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.common.PublishMessage.PublishPath;
import jp.piax.ofm.pubsub.common.TransPathInfo;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.SimpleMonitor;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.RemoteValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ALMPubSubAgent extends AbstructPubSubAgent implements PubSubAgentIf {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    static final Logger logger = LoggerFactory.getLogger(ALMPubSubAgent.class);

    private volatile PubSubMonitor monitor = new SimpleMonitor();

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
    }

    @Override
    public void onDeparture() {
        super.onDeparture();
    }


    @Override
    public void onArrival() {
        super.onArrival();
    }


    @Override
    public void onDestruction() {
        // nothing todo
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

        logger.debug("[{}] subscribe {}", getUserId(), topic);

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

            subscribedTopic.add(topic);
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

        logger.debug("[{}] unsubscribe topic:[{}]", getUserId(), topic);

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

            subscribedTopic.remove(topic);
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

        long publishid = publishIdCounter.incrementAndGet();


        // Overlay 経由の publish
        PublishMessage msg = new PublishMessage(userId, topic, content, publishid, null, PublishPath.Overlay, topic);

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
                        getUserId(), ste[1].getClassName(), ste[1].getLineNumber(), remote.getPeer());
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

        // 転送経路をコンソールに出力
        if (msg.getTransPath() != null) {
            StringBuilder buf = new StringBuilder();
            for (TransPathInfo pi : msg.getTransPath()) {
                buf.append(" " + pi);
            }
            System.out.println(buf.toString());
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
    private boolean isValidTopicName(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        return true;
    }

    /**
     * topic に対応する PIAX の属性を登録する
     * @param topic 
     */
    private void declaredAttribute(String topic) {
        _declaredAttributes(new String[]{topic});
    }

    /**
     * topic にオーバレイを対応づける
     * @param topic 
     */
    private void bindSubscribeOverlay(String topic) {
        _bindSubscribeOverlays(new String[]{topic});
    }
}
