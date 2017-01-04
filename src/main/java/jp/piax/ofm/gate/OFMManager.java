package jp.piax.ofm.gate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import jp.piax.ofm.gate.common.OFMCtrlAPIException;
import jp.piax.ofm.gate.common.SetupOFMResult;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.gate.messages.OFMSubscribeKeepAlive;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.json.JSONException;
import org.piax.gtrans.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Openflow multicast の状態を保持・管理するクラス
 */
public class OFMManager {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(OFMManager.class);

    /**
     * ある topic に対する Openflow multicast 情報を保持するクラス
     * OFM アドレスと subscriber の OFM 受信アドレスの対応を管理する
     */
    protected static class OFMEntry {
        private String topic;   // トピック名
        private InetSocketAddress OFMaddress;   // トピック名に対応する OFM アドレス
        // OFM 受信アドレスとそのアドレスを介して受信している subscriber のOFMポート
        private Set<IPMACPair> subscriberAddrs = new HashSet<IPMACPair>();

        /**
         * コンストラクタ
         * @param topic トピック名
         */
        public OFMEntry(String topic) {
            if (topic == null)
                throw new NullPointerException("topic should not be null");
            if (topic.isEmpty())
                throw new IllegalArgumentException("topic should not be empty");

            this.topic = topic;
        }

        /**
         * コンストラクタ
         * @param topic トピック名
         * @param ofmaddress topic に対応する OFM アドレス
         * @param subscribercount topic に対応するsubscriberのOFM受信アドレスのSet
         */
        public OFMEntry(String topic, InetSocketAddress ofmaddress,
                Set<IPMACPair> subscriberaddrs) {
            if (topic == null)
                throw new NullPointerException("topic should not be null");
            if (topic.isEmpty())
                throw new IllegalArgumentException("topic should not be empty");
            if (ofmaddress == null)
                throw new NullPointerException("ofmaddress should not be null");
            if (subscriberaddrs == null)
                throw new NullPointerException("subscriberaddrs should not be null");
            if (subscriberaddrs.isEmpty())
                throw new IllegalArgumentException("subscriberaddrs should not be empty");

            this.topic = topic;
            this.OFMaddress = ofmaddress;
            this.subscriberAddrs.addAll(subscriberaddrs);
        }

        /**
         * topic を取得する
         * @return
         */
        public String getTopic() {
            return topic;
        }

        /**
         * OFM アドレスの有無を返す
         * @return true OFM アドレス設定済み false OFM アドレス未設定
         */
        synchronized boolean hasOFMAddress() {
            return (OFMaddress != null);
        }

        /**
         * OFM アドレスを設定する
         * 既に OFM アドレスが設定されている場合は null を設定する場合
         * を除き、 IllegalStateException が生じる
         * 
         * @param address OFM アドレス
         * @param subscribers
         */
        synchronized void setOFMSet(InetSocketAddress address,
                Set<IPMACPair> subscribers) {
            if (address == null)
                throw new NullPointerException("address shoud not be null");
            if (subscribers == null)
                throw new NullPointerException("subscribers shoud not be null");

            OFMaddress = address;
            subscriberAddrs.addAll(subscribers);
        }

        /**
         * OFM アドレスを取得する
         * @return OFM アドレス
         */
        synchronized InetSocketAddress getOFMAddress() {
            return OFMaddress;
        }

        /**
         * OFM アドレスと subscriber アドレスを削除する
         */
        synchronized void clearOFM() {
            OFMaddress = null;
            subscriberAddrs.clear();
        }

        /**
         * subscriber の OFM 受信アドレスを追加する
         * @param subscriber
         */
        synchronized void addSubscriber(IPMACPair subscriber) {
            if (subscriber == null)
                throw new NullPointerException("subscriber should not be null");

            subscriberAddrs.add(subscriber);
        }

        /**
         * subscriber の OFM 受信アドレスを追加する
         * @param subscribers
         */
        synchronized void addSubscribers(Set<IPMACPair> subscribers) {
            if (subscribers == null)
                throw new NullPointerException("subscribers should not be null");

            subscriberAddrs.addAll(subscribers);
        }


        /**
         * subscriber の OFM 受信アドレスを削除する
         * @param subscriber
         */
        synchronized void removeSubscriber(IPMACPair subscriber) {
            if (subscriber == null)
                throw new NullPointerException("subscriber should not be null");

            subscriberAddrs.remove(subscriber);
        }

        /**
         * subscriber の OFM 受信アドレスを削除する
         * @param subscribers
         */
        public void removeSubscribers(Set<IPMACPair> subscribers) {
            if (subscribers == null)
                throw new NullPointerException("subscribers should not be null");

            subscriberAddrs.removeAll(subscribers);
        }

        /**
         * subscriber が登録されているか否かを返す
         * @param subscriber
         * @return true 登録されている false 登録されていない
         */
        synchronized boolean containsSubscriber(IPMACPair subscriber) {
            if (subscriber == null)
                throw new NullPointerException("subscriber should not be null");

            return subscriberAddrs.contains(subscriber);
        }

        /**
         * subscriber の OFM 受信アドレスの snapshot を取得する
         * @return subscriber の OFM 受信アドレス 
         */
        synchronized Set<IPMACPair> getSubscribersOFMAddressSnapshot() {
            return new HashSet<IPMACPair>(subscriberAddrs);
        }

        /**
         * subscriber が空か否かを返す
         * @return true 空 false メンバー有
         */
        synchronized boolean subscriberEmpty() {
            return subscriberAddrs.isEmpty();
        }
    }


    /**
     * 外部からの参照用 OFMEntry
     */
    public class OFMTableEntry {
        /** トピック名 */
        private String topic;
        /** トピック名に対応する OFM アドレス */
        private InetSocketAddress OFMaddress;
        /** OFM 受信アドレスとそのアドレスを介して受信している subscriber のOFMポート */
        private Set<IPMACPair> subscriberAddrs = null;

        /**
         * コンストラクタ
         * @param entry 元となる OFMEntry
         */
        public OFMTableEntry(OFMEntry entry) {
            if (entry == null)
                throw new NullPointerException("entry should not be null");

            this.topic = entry.getTopic();
            this.OFMaddress = entry.getOFMAddress();
            this.subscriberAddrs = entry.getSubscribersOFMAddressSnapshot();
        }

        /**
         * topic を取得する
         * @return
         */
        public String getTopic() {
            return topic;
        }

        /**
         * OFM アドレスを取得する
         * @return OFM アドレス
         */
        public synchronized InetSocketAddress getOFMAddress() {
            return OFMaddress;
        }

        /**
         * subscriber の OFM 受信アドレスの snapshot を取得する
         * @return subscriber の OFM 受信アドレスの UnmodifiableSet
         */
        public synchronized Set<IPMACPair> getSubscribersOFMAddress() {
            return Collections.unmodifiableSet(subscriberAddrs);
        }
    }

    protected OFMCtrlClient ctrlClient;     // Openflow コントローラ操作クライアント
    protected Transport<OFMUdpLocator> ofmTransport;   // subscriber への keep alive に使う transport
    // topic と OFM 管理情報の map
    protected final ConcurrentHashMap<String, OFMEntry> managedTopics = new ConcurrentHashMap<>();

    protected long keepaliveWaitTime;    // Keep-alive メッセージ送信待機時間 (ms)

    /**
     * コンストラクタ
     * @param ofmControllerEndpoint Openflow コントローラ WebAPI endpoint
     * @param ofmtransport subscriber への keep alive に使う transport
     * @param keepalivewait Keep-alive メッセージ送信待機時間 (ms) 0 以下を与えた場合は Keep-Alive 機能は使用しない
     * @param contimeout OFM サーバへの接続タイムアウト (ms)
     * @param reqtimeout OFM 設定 Web APIの要求処理タイムアウト (ms)
     */
    public OFMManager(URI ofmControllerEndpoint, Transport<OFMUdpLocator> ofmtransport, 
            long keepalivewait, int contimeout, int reqtimeout) {
        if (ofmControllerEndpoint == null)
            throw new NullPointerException("ofmControllerEndpoint should not be null");
        if (ofmtransport == null)
            throw new NullPointerException("ofmtransport should not be null");
        if (!ofmtransport.isUp())
            throw new IllegalArgumentException("ofmtransport should not be closed");
        if (contimeout < 0)
            throw new IllegalArgumentException("contimeout should be positive");
        if (reqtimeout < 0)
            throw new IllegalArgumentException("reqtimeout should be positive");

        ctrlClient = new OFMCtrlClient(ofmControllerEndpoint, contimeout, reqtimeout);
        ofmTransport = ofmtransport;
        keepaliveWaitTime = keepalivewait;

        if (0 < keepaliveWaitTime) {
            // keep alive メッセージ発行スレッド
            Timer t = new Timer("KeepAliveTask", true);
            t.schedule(new TimerTask() {
                @Override
                public void run() {
                    for (Map.Entry<String, OFMEntry> pair : managedTopics.entrySet()) {
                        String topic = pair.getKey();
                        InetSocketAddress ofmaddress = pair.getValue().getOFMAddress();
                        if (ofmaddress == null) {
                            continue;
                        }
                        OFMSubscribeKeepAlive keepalive = new OFMSubscribeKeepAlive(topic, ofmaddress);
                        try {
                            logger.debug("send Keep-alive message for topic:[{}] to OFM addr:{}", topic, ofmaddress);
                            ofmTransport.send(OFMPubSubOverlay.DEFAULT_TRANSPORT_ID, new OFMUdpLocator(ofmaddress), keepalive);
                        } catch (IOException e) {
                            logger.error("IOException at sending Keep-alive message to {}", ofmaddress);
                        }
                    }
                }
            }, 0, keepaliveWaitTime);
        }
    }

    /**
     * topic に対応する OFM アドレスを取得する
     * @param topic 
     * @return topic に対応する OFM アドレス topic 未登録の場合は null
     */
    public InetSocketAddress getOFMAddress(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("getOFMAddress for topic:[{}]", topic);

        OFMEntry entry = managedTopics.get(topic);
        if (entry == null) {
            logger.warn("Topic:[{}] is not registered", topic);
            return null;
        }

        return entry.getOFMAddress();
    }

    /**
     * Openflow multicast を設定する
     * @param topic 対象の topic
     * @param subscribers OFM 受信アドレスと対応する subscriber の OFM ポート情報
     * @return OFM アドレスや成否情報を含む SetupOFMResult
     * @throws JSONException 
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     */
    public SetupOFMResult setupOFM(String topic,  Set<IPMACPair> subscribers) throws JSONException, OFMCtrlAPIException, IOException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (subscribers == null)
            throw new NullPointerException("subscribers should not be null");
        if (subscribers.isEmpty())
            throw new IllegalArgumentException("subscribers should not be empty");

        logger.debug("setupOFM for topic:[{}] , {} addresses", topic, subscribers.size());

        managedTopics.putIfAbsent(topic, new OFMEntry(topic));
        OFMEntry entry = managedTopics.get(topic);
        synchronized (entry) {
            if (!entry.hasOFMAddress()) {
                // setupOFM 要求
                InetSocketAddress ofmaddress = ctrlClient.setupOFM(subscribers);    // possibly takes long time...
                if (ofmaddress != null) {
                    entry.setOFMSet(ofmaddress, subscribers);
                    return new SetupOFMResult(topic, true, ofmaddress, subscribers);
                } else {
                    logger.error("failed to setupOFM for [{}]", topic);
                    return new SetupOFMResult(topic, false, null, null);
                }
            } else {
                Set<IPMACPair> reged_subs = entry.getSubscribersOFMAddressSnapshot();
                // 要求された OFM の配信先が現状の配信先と一致していた場合はアドレスを返す
                if (reged_subs.containsAll(subscribers) && reged_subs.size() == subscribers.size()) {
                    return new SetupOFMResult(topic, true, entry.getOFMAddress(), subscribers);
                } else {
                    // それ以外は warn として setupOFM のリクエストが完全に処理されていないことを返す
                    logger.warn("topic [{}] is already registered", topic);
                    return new SetupOFMResult(topic, false, entry.getOFMAddress(), reged_subs);
                }
            }
        }
    }

    /**
     * topic に対する Openflow multicast を停止する
     * @param topic Openflow multicast を停止する topic
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     */
    public void releaseOFM(String topic) throws OFMCtrlAPIException, IOException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");

        logger.debug("releaseOFM for topic:[{}]", topic);

        OFMEntry entry = managedTopics.get(topic);
        if (entry == null) {
            logger.warn("Topic:[{}] is not registered", topic);
            return;
        }

        synchronized (entry) {
            if (!entry.hasOFMAddress()) {
                logger.warn("releaseOFM is called but topic:[{}] has no OFM address", topic);
                return;
            }

            InetSocketAddress ofmaddress = entry.getOFMAddress();
            entry.clearOFM();
            ctrlClient.clearOFMREST(ofmaddress);     // possibly takes long time...
        }
    }

    /**
     * topic に対応する Openflow multicast グループに subscriber を追加する
     * 既に Openflow multicast グループに含まれている場合は無視される
     * @param topic 追加対象の topic 
     * @param receiveaddress 追加する subscriber の OFM 受信アドレス
     * @return topic に対応する OFM アドレス, topic に対応する OFM アドレスが無い場合は null
     * @throws JSONException 
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     */
    public InetSocketAddress subscribeRequest(String topic, IPMACPair receiveaddress) throws JSONException, OFMCtrlAPIException, IOException {
        return subscribeRequest(topic, Collections.singleton(receiveaddress));
    }

    /**
     * topic に対応する Openflow multicast グループに subscriber を追加する
     * 既に Openflow multicast グループに含まれている場合は無視される
     * @param topic 追加対象の topic 
     * @param receiveaddress 追加する subscriber の OFM 受信アドレスの Set
     * @return topic に対応する OFM アドレス, topic に対応する OFM アドレスが無い場合は null
     * @throws JSONException 
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     */
    public InetSocketAddress subscribeRequest(String topic, Set<IPMACPair> receiveaddress) throws JSONException, OFMCtrlAPIException, IOException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (receiveaddress == null)
            throw new NullPointerException("receiveaddress should not be null");
        if (receiveaddress.isEmpty())
            throw new IllegalArgumentException("receiveaddress should not be empty");

        Date Time = new Date();
        Iterator iter;
        
        if (logger.isDebugEnabled()) {
        	iter = receiveaddress.iterator();
        	while(iter.hasNext()){
        		System.out.println(Time.toLocaleString() + "subscribeRequest from "+iter.next());
        	}
        	logger.debug("subscribeRequest for topic:[{}] adding receivers:{}", topic, IPMACPair.convertToOFMAddrString(receiveaddress));
        }

        OFMEntry entry = managedTopics.get(topic);
        if (entry == null) {
            logger.warn("Topic:[{}] is not registered", topic);
            return null;
        }

        synchronized (entry) {
            // setupOFM が行われているか
            if (!entry.hasOFMAddress()) {
                logger.warn("Topic:[{}] does not have OFM address", topic);
                return null;
            }

            // 登録が必要な subscriber ( subscribers - reged_subscribers )
            Set<IPMACPair> reged_subscribers = entry.getSubscribersOFMAddressSnapshot();
            iter = reged_subscribers.iterator();
            while(iter.hasNext()){
            	System.out.println("reged_subscriber: "+iter.next());
            }
            Set<IPMACPair> new_subscribers = new HashSet<IPMACPair>(receiveaddress);
            iter = new_subscribers.iterator();
            while(iter.hasNext()){
        	   System.out.println("new_subscriber: "+iter.next());
            }
            new_subscribers.removeAll(reged_subscribers);

            if (!new_subscribers.isEmpty()) {
                InetSocketAddress ofmaddress = entry.getOFMAddress();
                logger.debug("OFM address {} for topic:[{}] will be added {} receivers ", ofmaddress, topic, new_subscribers.size());
                ctrlClient.addOFMMembers(ofmaddress, new_subscribers);  // possibly takes long time...
                entry.addSubscribers(new_subscribers);
                logger.info("OFM address {} for topic:[{}] was added {} receivers ", ofmaddress, topic, new_subscribers.size());
            }
            if(new_subscribers.isEmpty()){
            	System.out.println(Time.toLocaleString() + " empty the new_subscriber");
            }
            return entry.getOFMAddress();
        }
    }

    /**
     * topic に対応する Openflow multicast グループから subscriber を削除する
     * Openflow multicast グループに含まれていない subscriber を与えた場合は無視される
     * この操作によって Openflow multicast グループが空になった場合は、 OFM アドレスは無効化される
     * @param topic 除去対象の topic 
     * @param receiveaddress 除去する subscriber の OFM 受信アドレス
     * @return topic に対応する OFM アドレス, OFM アドレスが無効化された場合は null
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     * @throws JSONException 
     */
    public InetSocketAddress unsubscribeRequest(String topic, IPMACPair receiveaddress) throws OFMCtrlAPIException, IOException, JSONException {
        return unsubscribeRequest(topic, Collections.singleton(receiveaddress));
    }

    /**
     * topic に対応する Openflow multicast グループから subscriber を削除する
     * Openflow multicast グループに含まれていない subscriber を与えた場合は無視される
     * この操作によって Openflow multicast グループが空になった場合は、 OFM アドレスは無効化される
     * @param topic 除去対象の topic 
     * @param receiveaddress 除去する subscriber の OFM 受信アドレスの Set
     * @return topic に対応する OFM アドレス, OFM アドレスが無効化された場合は null
     * @throws OFMCtrlAPIException 
     * @throws IOException 
     * @throws JSONException 
     */
    public InetSocketAddress unsubscribeRequest(String topic, Set<IPMACPair> receiveaddress) throws OFMCtrlAPIException, IOException, JSONException {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (receiveaddress == null)
            throw new NullPointerException("receiveaddress should not be null");
        if (receiveaddress.isEmpty())
            throw new IllegalArgumentException("receiveaddress should not be empty");

    	Date Time = new Date();
    	Iterator iter;
        
        if (logger.isDebugEnabled()) {
        	iter = receiveaddress.iterator();
        	while(iter.hasNext()){
        		System.out.println(Time.toLocaleString() + " unsubscribeRequest from " + iter.next());
        	}
        	logger.debug("unsubscribeRequest for topic:[{}] removing receivers:{}", topic, IPMACPair.convertToOFMAddrString(receiveaddress));
        }

        OFMEntry entry = managedTopics.get(topic);
        if (entry == null) {
            logger.warn("Topic:[{}] is not registered", topic);
            return null;
        }

        synchronized (entry) {
            // setupOFM が行われているか
            if (!entry.hasOFMAddress()) {
                logger.warn("Topic:[{}] does not have OFM address", topic);
                return null;
            }

            InetSocketAddress ofmaddress = entry.getOFMAddress();

            // 削除が必要な subscriber ( reged_subscribers and subscribers )
            Set<IPMACPair> reged_subscribers = entry.getSubscribersOFMAddressSnapshot();
            Set<IPMACPair> remove_subscribers = new HashSet<IPMACPair>(reged_subscribers);
            remove_subscribers.retainAll(receiveaddress);
            
            iter = remove_subscribers.iterator();
            while(iter.hasNext()){
        	   System.out.println("must remove subscriber: "+iter.next());
            }

            if (remove_subscribers.isEmpty()) {
            	System.out.println(Time.toLocaleString() + " remove_subscriber is empty");
                logger.warn("No removable address is registered [{}]", topic);
                return ofmaddress;
            }

            if (remove_subscribers.size() == reged_subscribers.size()) {
                // topic に対応する subscriber が無くなる場合は clearOFM を call
                logger.debug("It becomes no receivers for topic:[{}]. Clear OFM address {}", topic, ofmaddress);
                ctrlClient.clearOFMREST(ofmaddress);     // possibly takes long time...
                entry.clearOFM();
                logger.info("OFM address {} for topic:[{}] is cleared", ofmaddress, topic);
                return null;
            } else {
                // 他の subscriber が残っている場合は removeOFMMember を call
                logger.debug("OFM address {} for topic:[{}] will be removed {} receivers ", ofmaddress, topic, remove_subscribers.size());
                ctrlClient.removeOFMMembers(ofmaddress, remove_subscribers); // possibly takes long time...
                entry.removeSubscribers(remove_subscribers);
                logger.info("OFM address {} for topic:[{}] was removed {} receivers ", ofmaddress, topic, remove_subscribers.size());
                return ofmaddress;
            }
        }
    }

    /**
     * OFMManager が把握している Topic - OFM address - OFM subscriber address を取得する
     * @return
     */
    public Set<OFMTableEntry> getOFMTable() {
        Set<OFMTableEntry> result = new HashSet<>();
        for (OFMEntry entry : managedTopics.values()) {
            result.add(new OFMTableEntry(entry));
        }
        return result;
    }
}
