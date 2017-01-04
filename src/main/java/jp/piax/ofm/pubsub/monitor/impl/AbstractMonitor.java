package jp.piax.ofm.pubsub.monitor.impl;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;

public abstract class AbstractMonitor implements PubSubMonitor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMonitor.class);
    private static final long serialVersionUID = 1L;

    transient protected OFMAddressCache ofmAddressCache;    // topic に対応する OFM アドレスのキャッシュ

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#attachOFMAddressCache(jp.piax.ofm.pubsub.misc.OFMAddressCache)
     */
    public synchronized void attachOFMAddressCache(OFMAddressCache cache) {
        if (cache == null)
            throw new NullPointerException("cache should not be null");

        ofmAddressCache = cache;
    }
    

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#setOFMAddress(java.lang.String, java.net.InetSocketAddress)
     */
    @Override
    public void setOFMAddress(String topic, InetSocketAddress ofmaddress) {
        if (topic == null || topic.isEmpty()) {
            logger.error("setOFMAddress : topic is null or empty [{}]", topic);
            return;
        }
        if (ofmaddress == null) {
            logger.debug("setOFMAddress [{}] : ofmaddress is null. clear OFM address", topic);
            ofmAddressCache.invalidate(topic);
            return;
        }
        logger.debug("setOFMAddress for [{}] sets {}", topic, ofmaddress);
        ofmAddressCache.put(topic, ofmaddress);
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.pubsub.monitor.PubSubMonitor#getOFMAddressCache(java.lang.String)
     */
    public InetSocketAddress getOFMAddress(String topic) {
        return ofmAddressCache.get(topic);
    }
}
