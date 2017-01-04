package jp.piax.ofm.pubsub.misc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ピア上の Subscriber のカウンタ
 */
public class SubscriberCounter {
    private Map<String, AtomicInteger> subscribers = new HashMap<>();

    /**
     * topic のsubscribe参照カウンタをインクリメントし、OFGate に通知が必要か否かを返す
     * @param topic topic名
     * @return OFGate に subscribe 通知が必要な場合は true
     */
    public synchronized boolean subscribeAndNeedUpdate(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        if (subscribers.containsKey(topic)) {
            // カウントアップのみ
            AtomicInteger count = subscribers.get(topic);
            count.incrementAndGet();
            return false;
        } else {
            // 未登録の場合はピア上で初のsubscriberなので
            // true を返してOFGate に通知させる
            subscribers.put(topic, new AtomicInteger(1));
            return true;
        }
    }

    /**
     * topic のsubscribe参照カウンタをデクリメントし、OFGate に通知が必要か否かを返す
     * @param topic topic名
     * @return OFGate に unsubscribe 通知が必要な場合は true
     */
    public synchronized boolean unsubscribeAndNeedUpdate(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        if (subscribers.containsKey(topic)) {
            AtomicInteger count = subscribers.get(topic);
            // == 0 はピア上にsubscriberがいなくなったことを意味するので
            // true を返してOFGate に通知させる
            if (0 == count.decrementAndGet()) {
                subscribers.remove(topic);
                return true;
            }
            return false;
        } else {
            // 未登録の場合は nothing to do
            return false;
        }
    }
}
