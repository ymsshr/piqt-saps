package jp.piax.ofm.pubsub.piax.trans;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.gate.messages.OFMSubscribeKeepAlive;
import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.Endpoint;
import org.piax.common.ObjectId;
import org.piax.common.TransportId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.FutureQueue;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.Overlay;
import org.piax.gtrans.ov.OverlayListener;
import org.piax.gtrans.ov.OverlayReceivedMessage;
import org.piax.gtrans.ProtocolUnsupportedException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.RequestTransport;
import org.piax.gtrans.TransOptions;
import org.piax.gtrans.Transport;
import org.piax.gtrans.TransportListener;
import org.piax.gtrans.ov.compound.CompoundOverlay.SpecialKey;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.impl.NestedMessage;
import org.piax.gtrans.ov.impl.OverlayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OFMPubSubOverlay extends OverlayImpl<StringKey, StringKey> implements
    OverlayListener<StringKey, StringKey> {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(OFMPubSubOverlay.class);

    public static TransportId DEFAULT_TRANSPORT_ID = new TransportId("ofmpubsub");
    /** loopback 待機タスク、OFM address cache の GC 周期のデフォルト値 (ms) */
    public static final long DEFAULT_GC_PERIOD = 30*1000;

    /** loopback 待機の有効時間 (ms) */
    public static long LoopbackTimeout = 1000;
    /** loopback 待機タスク、OFM address cache の GC 周期 (ms) */
    public static long GcPeriod = DEFAULT_GC_PERIOD;

    /**
     * OFM 用キーのprefix このprefixを持つキーは OFM 用となり、
     * ALM(DOLR)には登録されない
     */
    public final static String OFM_KEY_PREFIX = "$OFM$.";

    protected Transport<OFMUdpLocator> ofmTransport;
    protected DOLR<StringKey> dolr;

    protected Set<StringKey> keys = new HashSet<StringKey>();
    protected IPMACPair ofmAddress;     // Openflow multicast 受信アドレス
    protected OFMAddressCache ofmAddressCache;  // topic に対応する OFM アドレスのキャッシュ

    // OFM loopback 処理用
    private final static Timer gcTimer = new Timer("loopbacktaskGC", true);

    private TimerTask gcTask;
    private final Map<String, LoopbackWaiting> loopbackWaitings = new ConcurrentHashMap<>();

    /**
     * OFM Loopback 待機タスク
     */
    class LoopbackWaiting {
        final long timeStamp = System.currentTimeMillis();
        private final PublishMessage pmsg;

        /**
         * コンストラクタ
         * @param msg loopback を待つ PublishMessage
         */
        public LoopbackWaiting(PublishMessage msg) {
            if (msg == null)
                throw new NullPointerException("msg should not be null");

            this.pmsg = msg;
        }

        /**
         * タスクが失効（待機期限切れ）したかどうかを判定する。
         * 
         * @return タスクが失効した場合 true、それ以外は false
         */
        boolean isExpired() {
            return System.currentTimeMillis() > timeStamp + LoopbackTimeout;
        }

        /**
         * OFM Loopback を受信した場合に呼び出す
         * これにより受信待機している PublishMessage が解放される
         */
        void receiveLoopback() {
            pmsg.receiveLoopbackMsg();
        }
    }

    /**
     * コンストラクタ
     * @param dolr PubSub に用いる DOLR オーバレイ
     * @param ofmtransport Openflow multicast 受信アドレスに対応する transport
     * @param cache トピックに対する OFM アドレスのキャッシュ
     * @throws IdConflictException
     * @throws SocketException 
     */
    public OFMPubSubOverlay(DOLR<StringKey> dolr, Transport<OFMUdpLocator> ofmtransport, OFMAddressCache cache) throws IdConflictException, SocketException {
        this(dolr, ofmtransport, cache, DEFAULT_TRANSPORT_ID);
    }

    /**
     * コンストラクタ
     * @param dolr PubSub に用いる DOLR オーバレイ
     * @param ofmtransport Openflow multicast 受信アドレスに対応する transport
     * @param cache トピックに対する OFM アドレスのキャッシュ
     * @param transid TransportId
     * @throws IdConflictException
     * @throws SocketException 
     */
    public OFMPubSubOverlay(DOLR<StringKey> dolr, Transport<OFMUdpLocator> ofmtransport, OFMAddressCache cache, TransportId transid) throws IdConflictException, SocketException {
        super(dolr.getPeer(), transid, dolr);

        if (ofmtransport == null)
            throw new NullPointerException("ofmtransport should not be null");
        if (cache == null)
            throw new NullPointerException("cache should not be null");

        this.ofmTransport = ofmtransport;
        this.dolr = dolr;
        this.ofmAddressCache = cache;

        OFMUdpLocator locator = (OFMUdpLocator) ofmTransport.getEndpoint();
        NetworkInterface ni = NetworkInterface.getByInetAddress(locator.getInetAddress());
        ofmAddress = new IPMACPair(locator.getSocketAddress(), ni.getHardwareAddress());

        this.dolr.setListener(this.getTransportId(), this);
        this.ofmTransport.setListener(this.getTransportId(), new TransportListener<OFMUdpLocator>() {
            @Override
            public void onReceive(Transport<OFMUdpLocator> trans, ReceivedMessage rmsg) {
                Object inner = rmsg.getMessage();

                if (inner instanceof OFMSubscribeKeepAlive) {
                    // receive OFM address keep-alive
                    OFMSubscribeKeepAlive msg = (OFMSubscribeKeepAlive) inner;
                    ofmAddressCache.put(msg.getTopic(), msg.getOFMAddress());
                    logger.trace("Receive a keep-alive message for [{}], address {}", msg.getTopic(), msg.getOFMAddress());
                } else if (inner instanceof OFMPublishMessage) {
                    // receive OFM publish
                    OFMPublishMessage pmsg = (OFMPublishMessage) inner;
                    StringKey targetkey = pmsg.getTopic();

                    // loopback check
                    LoopbackWaiting task = loopbackWaitings.get(getTag(pmsg.getUserId(), pmsg.getPublishId()));
                    if (task != null) {
                        if(!task.isExpired()) {
                            task.receiveLoopback();
                        }
                        loopbackWaitings.remove(task);
                    }

                    Set<StringKey> matched = new HashSet<StringKey>();
                    synchronized (OFMPubSubOverlay.this.keys) {
                        if (OFMPubSubOverlay.this.keys.contains(targetkey)) {
                            matched.add(targetkey);
                        }
                    }
                    logger.trace("Receive a OFM publish message for [{}]", pmsg.getTopic());

                    NestedMessage nmsg = (NestedMessage) pmsg.getMessage();

                    OverlayReceivedMessage<StringKey> rcvMsg = new OverlayReceivedMessage<StringKey>(
                            rmsg.getSender(), rmsg.getSource(), matched, nmsg);
                    OFMPubSubOverlay.this.onReceiveRequest(null, rcvMsg);
                } else {
                    logger.warn("Unknown message from {} {} : {}", rmsg.getSender(), rmsg.getSource(), inner);
                }
            }
        });

        // gcTaskのセット
        gcTask = new TimerTask() {
            @Override
            public void run() {
                logger.debug("run GC");
                try {
                    for (Map.Entry<String, LoopbackWaiting> ent : loopbackWaitings.entrySet()) {
                        LoopbackWaiting t = ent.getValue();
                        if (t.isExpired()) {
                            loopbackWaitings.remove(ent.getKey());
                        }
                    }
                    ofmAddressCache.expire();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        };
        gcTimer.schedule(gcTask, GcPeriod / 2, GcPeriod);
    }


    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.TransportImpl#fin()
     */
    @Override
    public void fin() {
        super.fin();
        gcTask.cancel();
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.Transport#getEndpoint()
     */
    @Override
    public Endpoint getEndpoint() {
        return dolr.getEndpoint();
    }


    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#join(java.util.Collection)
     */
    @Override
    public boolean join(Collection<? extends Endpoint> seeds) throws IOException {
        if (dolr.isJoined())
            return true;

        return dolr.join(seeds);
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#leave()
     */
    @Override
    public boolean leave() throws IOException {
        if (!dolr.isJoined())
            return true;

        return dolr.leave();
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#isJoined()
     */
    @Override
    public boolean isJoined() {
        return dolr.isJoined();
    }

    
    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#getAvailableKeyType()
     */
    @Override
    public Class<?> getAvailableKeyType() {
        return Comparable.class;
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#lowerAddKey(org.piax.common.Key)
     */
    @Override
    protected void lowerAddKey(StringKey key) throws IOException {
        logger.debug("lower addKey:{}", key);
        synchronized (keys) {
            keys.add(key);
        }
        if (!key.getKey().startsWith(OFM_KEY_PREFIX)) {
            dolr.addKey(this.getTransportId(), key);
        }
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.impl.OverlayImpl#lowerRemoveKey(org.piax.common.Key)
     */
    @Override
    protected void lowerRemoveKey(StringKey key) throws IOException {
        logger.debug("lower removeKey:{}", key);
        synchronized (keys) {
            keys.remove(key);
        }
        if (!key.getKey().startsWith(OFM_KEY_PREFIX)) {
            dolr.removeKey(this.getTransportId(), key);
        }
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.RequestTransport#request(org.piax.common.ObjectId, org.piax.common.ObjectId, org.piax.common.Destination, java.lang.Object, int)
     */
    @Override
    public FutureQueue<?> request(ObjectId sender, ObjectId receiver,
            StringKey dst, Object msg, int timeout)
            throws ProtocolUnsupportedException, IOException {
        logger.debug("request peer:{} dst:{}", this.getPeer().getPeerId(), dst);
        logger.trace("msg:{}", msg);
        try {
            if (!(dst instanceof StringKey)) {
                throw new ProtocolUnsupportedException("OFMPubSubOverlay only supports String destination");
            }

            NestedMessage nmsg = new NestedMessage(sender, receiver, null,
                    getEndpoint(), msg);

            // PublishMessage の取り出しと Publish 経路の判定
            Object wrappedmsg = msg;
            while (wrappedmsg instanceof NestedMessage) {
                wrappedmsg = ((NestedMessage) wrappedmsg).getInner();
            }
            if (wrappedmsg instanceof AgentHomeImpl.AgCallPack) {
                AgentHomeImpl.AgCallPack callpack = (AgentHomeImpl.AgCallPack) wrappedmsg;
                if ("onReceivePublish".equals(callpack.method) && callpack.args.length == 1) {
                    PublishMessage pmsg = (PublishMessage) callpack.args[0];

                    if (pmsg.getPublishPath() == PublishMessage.PublishPath.OFM) {
                        // OFM 経由での publish

                        // loopback 待ちのため Future をセットしておく see onReceive in constructor
                        LoopbackWaiting task = new LoopbackWaiting(pmsg);
                        loopbackWaitings.put(getTag(pmsg.getUserId(), pmsg.getPublishId()), task);

                        // OFMアドレスに UDP で送信する
                        OFMPublishMessage omsg = new OFMPublishMessage(dst, pmsg.getUserId(), pmsg.getPublishId(), nmsg);
                        OFMUdpLocator ofmlocator = new OFMUdpLocator(pmsg.getOfmAddress());
                        ofmTransport.send(this.getTransportId(), ofmlocator, omsg);
                        logger.info("OFM publish [{}] to {}", pmsg.getTopic(), ofmlocator);

                        // Openflow multicast 時は return なし
                        FutureQueue<?> future = new FutureQueue();
                        future.setEOFuture();
                        return future;
                    }
                    logger.info("Overlay publish [{}]", pmsg.getTopic());
                }
            }
            return dolr.request(transId, dst, nmsg, timeout);
        } finally {
            logger.trace("EXIT:");
        }
    }

    private String getTag(String userid, long publishid) {
        return userid + "+" + publishid;
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.OverlayListener#onReceive(org.piax.gtrans.Overlay, org.piax.gtrans.OverlayReceivedMessage)
     */
    @Override
    public void onReceive(Overlay<StringKey, StringKey> trans,
            OverlayReceivedMessage<StringKey> rmsg) {
        Set<StringKey> matched = new HashSet<StringKey>();
        for (StringKey k : rmsg.getMatchedKeys()) {
            matched.add(k);
        }
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.debug("matchedKeys:{} nmsg:{}", matched, nmsg);

        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<StringKey> keys = getKeys(nmsg.receiver);
        keys.retainAll(matched);
        if (keys.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return;
            }
        }
        OverlayListener<StringKey, StringKey> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return;
        }
        OverlayReceivedMessage<StringKey> rcvMsg = new OverlayReceivedMessage<StringKey>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());
        ovl.onReceive(this, rcvMsg);
    }

    /* (非 Javadoc)
     * @see org.piax.gtrans.OverlayListener#onReceiveRequest(org.piax.gtrans.Overlay, org.piax.gtrans.OverlayReceivedMessage)
     */
    @Override
    public FutureQueue<?> onReceiveRequest(Overlay<StringKey, StringKey> trans,
            OverlayReceivedMessage<StringKey> rmsg) {
        logger.trace("ENTRY:");
        Set<StringKey> matched = new HashSet<StringKey>();
        for (StringKey k : rmsg.getMatchedKeys()) {
            matched.add(k);
        }
        NestedMessage nmsg = (NestedMessage) rmsg.getMessage();
        logger.trace("matchedKeys:{} nmsg:{}", matched, nmsg);

        // matchしたkeyセットと upperが登録しているkeyセットの共通部分を求める
        Set<StringKey> keys = getKeys(nmsg.receiver);
        keys.retainAll(matched);
        if (keys.isEmpty()) {
            // gatewayのための処理
            if (nmsg.passthrough != SpecialKey.WILDCARD) {
                return FutureQueue.emptyQueue();
            }
        }
        OverlayListener<StringKey, StringKey> ovl = getListener(nmsg.receiver);
        if (ovl == null) {
            logger.info("onReceiveRequest data purged as no such listener");
            return FutureQueue.emptyQueue();
        }
        OverlayReceivedMessage<StringKey> rcvMsg = new OverlayReceivedMessage<StringKey>(
                nmsg.sender, nmsg.src, keys, nmsg.getInner());
        return ovl.onReceiveRequest(this, rcvMsg);
    }


    /* (非 Javadoc)
     * @see org.piax.gtrans.RequestTransportListener#onReceive(org.piax.gtrans.RequestTransport, org.piax.gtrans.ReceivedMessage)
     *
    @Override
    public void onReceive(RequestTransport<StringKey> trans, ReceivedMessage rmsg) {
        logger.warn("onReceive for RequestTransport is unsupported");
    }
    /* (非 Javadoc)
     * @see org.piax.gtrans.RequestTransportListener#onReceiveRequest(org.piax.gtrans.RequestTransport, org.piax.gtrans.ReceivedMessage)
     
    @Override
    public FutureQueue<?> onReceiveRequest(RequestTransport<StringKey> trans,
            ReceivedMessage rmsg) {
        logger.warn("onReceiveRequest for RequestTransport is unsupported");
        return null;
    }
*/
    /* (非 Javadoc)
     * @see org.piax.gtrans.TransportListener#onReceive(org.piax.gtrans.Transport, org.piax.gtrans.ReceivedMessage)
     */
    @Override
    public void onReceive(Transport<StringKey> trans, ReceivedMessage rmsg) {
        logger.warn("onReceive for Transport is unsupported");
    }

    @Override
    public FutureQueue<?> request(ObjectId arg0, ObjectId arg1, StringKey arg2,
            Object arg3, TransOptions arg4)
            throws ProtocolUnsupportedException, IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
