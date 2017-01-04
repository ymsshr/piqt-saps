package jp.piax.ofm.pubsub.misc;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * トピックに対する OFM アドレスのキャッシュ
 */
public class OFMAddressCache {
    private static final Logger logger = LoggerFactory.getLogger(OFMAddressCache.class);
    protected ConcurrentHashMap<String, CacheElement> cacheStore = new ConcurrentHashMap<String, CacheElement>();
    protected long timeout = 0;

    /**
     * cacheStore に格納するエレメント
     */
    private static class CacheElement {
        private String topic;
        private InetSocketAddress address;
        private long lastUpdate;

        public CacheElement(String topic, InetSocketAddress address,
                long lastupdate) {
            if (topic == null)
                throw new NullPointerException("topic should not be null");
            if (topic.isEmpty())
                throw new IllegalArgumentException("topic should not be empty");
            if (address == null)
                throw new NullPointerException("address should not be null");
            if (lastupdate < 0)
                throw new IllegalArgumentException("lastupdate should be positive");

            this.topic = topic;
            this.address = address;
            this.lastUpdate = lastupdate;
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastupdate) {
            if (lastupdate < 0)
                throw new IllegalArgumentException("lastupdate should be positive");

            this.lastUpdate = lastupdate;
        }

        public String getTopic() {
            return topic;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    /**
     * コンストラクタ
     * @param timeout キャッシュが無効になるまでの時間 (ms) 負数を指定すると無限に保持する
     */
    public OFMAddressCache(long timeout) {
        this.timeout = timeout;
    }

    /**
     * 指定された topic の OFM アドレスを取得する
     * キャッシュされていない場合、タイムアウトしている場合は null を返す
     * @param topic
     * @return OFM アドレス キャッシュされていない場合、タイムアウトしている場合は null
     */
    public InetSocketAddress get(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        CacheElement ele = cacheStore.get(topic);

        if (ele == null)
            return null;

        // timeout ?
        if (timeout < System.currentTimeMillis() - ele.getLastUpdate()) {
            cacheStore.remove(topic);
            return null;
        }
        return ele.getAddress();
    }

    /**
     * 指定された topic の OFM アドレスを格納する
     * 既に格納済みの場合は、最終更新時刻を更新する
     * @param topic
     * @param ofmaddress OFM アドレス
     */
    public void put(String topic, InetSocketAddress ofmaddress) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");

        // タイムアウト 0 の場合(キャッシュ保持しない)
        if (timeout == 0) {
            return;
        }

        // update if the entry exists
        CacheElement ele = cacheStore.get(topic);
        if (ele == null) {
            CacheElement newele = new CacheElement(topic, ofmaddress, System.currentTimeMillis());
            ele = cacheStore.putIfAbsent(topic, newele);
            if (ele == null) {
                logger.debug("Store cache of [{}]", topic);
            } else {
                ele.setLastUpdate(System.currentTimeMillis());
                logger.debug("Update cache of [{}]", topic);
            }
        } else {
            ele.setLastUpdate(System.currentTimeMillis());
            logger.debug("Update cache of [{}]", topic);
        }
    }

    /**
     * 指定された topic のキャッシュを破棄する
     * @param topic
     */
    public void invalidate(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        cacheStore.remove(topic);
        logger.debug("Invalidate cache of [{}]", topic);
    }

    /**
     * タイムアウトしたエントリを削除する
     */
    public void expire() {
        logger.debug("Expiring OFM cache. [{}]", cacheStore.size());
        long expiretime = System.currentTimeMillis();
        for (Map.Entry<String, CacheElement> ele : cacheStore.entrySet()) {
            if (timeout < expiretime - ele.getValue().getLastUpdate()) {
                cacheStore.remove(ele.getKey());
                logger.debug("Expire cache of [{}]", ele.getKey());
            }
        }
    }
}
