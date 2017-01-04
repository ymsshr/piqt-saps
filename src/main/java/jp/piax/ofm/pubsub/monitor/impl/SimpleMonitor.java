package jp.piax.ofm.pubsub.monitor.impl;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;

/**
 * PubSubMonitor のサンプル実装
 * 3回以上 publish && subscriber 1つ以上あると setupOFM を実施する
 */
public class SimpleMonitor extends AbstractMonitor implements PubSubMonitor {
    private static final long serialVersionUID = 1L;
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(SimpleMonitor.class);

    public static int OFM_PUBLISH_LIMIT = 3;    // setupOFM を実行する publish の最低回数

    protected Map<String, AtomicInteger> publishCounter = new HashMap<String, AtomicInteger>();
    protected Map<String, Set<SubscriberInfo>> subscribeInfo = new HashMap<String, Set<SubscriberInfo>>();

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#getMonitoringTopics()
     */
    @Override
    public synchronized Set<String> getMonitoringTopics() {
        return new HashSet<String>(publishCounter.keySet());
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#putPublish(java.lang.String)
     */
    @Override
    public synchronized void putPublish(String topic) {
        if (topic == null || topic.isEmpty()) {
            logger.error("putPublish : topic is null or empty [{}]", topic);
            return;
        }

        if (!publishCounter.containsKey(topic)) {
            publishCounter.put(topic, new AtomicInteger(1));
            logger.debug("putPublish : new topic [{}] 1", topic);
            return;
        } else {
            int count = publishCounter.get(topic).incrementAndGet();
            logger.debug("putPublish : topic [{}] {}", topic, count);
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#putSubscriberInfo(java.lang.String, java.util.Set)
     */
    @Override
    public synchronized void putSubscriberInfo(String topic, Set<SubscriberInfo> info) {
        if (topic == null || topic.isEmpty()) {
            logger.error("putSubscriberInfo : topic is null or empty [{}]", topic);
            return;
        }
        if (info == null) {
            logger.error("putSubscriberInfo for [{}] : info is null", topic);
            return;
        }

        logger.debug("putSubscriberInfo for [{}] store {} subscriber(s)", topic, info.size());
        subscribeInfo.put(topic, new HashSet<SubscriberInfo>(info));
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#useOFM(java.lang.String)
     */
    @Override
    public synchronized boolean useOFM(String topic) {
        if (topic == null || topic.isEmpty()) {
            logger.error("useOFM : topic is null or empty [{}]", topic);
            return false;
        }

        boolean result = (ofmAddressCache.get(topic) != null);
        logger.debug("useOFM for [{}] returns {}", topic, result);
        return result;
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#setupOFM(java.lang.String)
     */
    @Override
    public synchronized boolean setupOFM(String topic) {
        if (topic == null || topic.isEmpty()) {
            logger.warn("setupOFM : topic is null or empty [{}]", topic);
            return false;
        }
        InetSocketAddress ofmaddress = ofmAddressCache.get(topic);
        if (ofmaddress != null) {
            logger.debug("setupOFM : topic [{}] already has OFM address {}", topic, ofmaddress);
            return false;
        }
        int count = publishCounter.get(topic).get();
        Set<SubscriberInfo> subscribers = subscribeInfo.get(topic);
        // OFM_PUBLISH_LIMIT 以上の publish を行い、 subscriber が登録されている場合に setupOFM 実行
        boolean result = (OFM_PUBLISH_LIMIT <= count && !subscribers.isEmpty());
        logger.debug("setupOFM for [{}] returns {}", topic, result);
        return result;
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#getSubscriberAddress(java.lang.String)
     */
    @Override
    public synchronized Set<IPMACPair> getSubscriberAddress(String topic) {
        if (topic == null || topic.isEmpty()) {
            logger.error("getSubscriberAddress : topic is null or empty [{}]", topic);
            return Collections.emptySet();
        }

        Set<SubscriberInfo> info = subscribeInfo.get(topic);
        Set<IPMACPair> result = new HashSet<IPMACPair>();
        for (SubscriberInfo i : info) {
            result.add(i.getOfmReceiverAddress());
        }

        logger.debug("getSubscriberAddress for [{}] returns {} address(es)", topic, result.size());
        return result;
    }
}
