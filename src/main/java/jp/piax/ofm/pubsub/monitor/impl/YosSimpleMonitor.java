package jp.piax.ofm.pubsub.monitor.impl;

import java.net.InetSocketAddress;
import java.util.Set;

import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.monitor.YosPubSubMonitor;

/**
 * YosPubSubMonitor のサンプル実装
 *
 * OFM setup 後、 ALM topic 上の subscriber が 1 つ以上ある場合
 * OFM メンバーへの追加(addOFMMembers)を促す
 */
public class YosSimpleMonitor extends SimpleMonitor implements YosPubSubMonitor {
    private static final long serialVersionUID = 1L;

    public static int ALM_SUBSCRIBER_LIMIT = 1;    // addSubscriberToOFM が true を返す subscriber 数

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.YosPubSubMonitor#addSubscriberToOFM(java.lang.String)
     */
    @Override
    public synchronized boolean addSubscriberToOFM(String topic) {
        InetSocketAddress ofmaddress = ofmAddressCache.get(topic);
        Set<SubscriberInfo> subs = subscribeInfo.get(topic);

        if (ofmaddress != null && subs != null && ALM_SUBSCRIBER_LIMIT <= subs.size())
            return true;
        return false;
    }

}
