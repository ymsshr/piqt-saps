package jp.piax.ofm.pubsub.piax;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.misc.SubscriberCounter;

import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.TransportIdPath;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AgentHome implementation for OFM Pub/Sub
 */
public class PubSubAgentHomeImpl extends AgentHomeImpl {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(PubSubAgentHomeImpl.class);

    public static long ExecutorShutdownAwaitingTime = 30*1000;
    protected TransportIdPath ofmPubSub = null;     // PubSub に用いるオーバレイの TransportIdPath
    protected IPMACPair ofmReceiverAddress = null;  // Openflow multicast 受信アドレス
    protected OFMAddressCache ofmAddressCache = null;   // topic に対する OFM アドレスのキャッシュ
    protected SubscriberCounter subscriberCouter = null;    // ピア上の topic に対応する subscriber 数カウンタ
    protected ScheduledExecutorService scheduledExecutor;   // Agent 内などで使う汎用Executor
    protected int scheduledExecutorCoreThread = 10;           // scheduledExecutor の core thread 数

    /**
     * コンストラクタ
     * @param trans 下位Transport
     * @param agClassPath Agent class ファイルのパス
     * @param pubsub_idpath PubSub に用いるオーバレイの TransportIdPath
     * @param ofmrecvaddress Openflow multicast 受信アドレス
     * @param ofmaddresscache topic に対する OFM アドレスのキャッシュ
     * @throws IOException
     * @throws IdConflictException
     */
    public PubSubAgentHomeImpl(ChannelTransport<?> trans, File[] agClassPath,
            TransportIdPath pubsub_idpath, IPMACPair ofmrecvaddress,
            OFMAddressCache ofmaddresscache, SubscriberCounter subscribercouter) throws IOException, IdConflictException {
        super(trans, agClassPath);
        if (pubsub_idpath == null)
            throw new NullPointerException("dolr should not be null");
        if (ofmrecvaddress == null)
            throw new NullPointerException("ofmrecvaddress should not be null");
        if (ofmaddresscache == null)
            throw new NullPointerException("ofmaddresscache should not be null");
        if (subscribercouter == null)
            throw new NullPointerException("subscribercouter should not be null");

        this.ofmPubSub = pubsub_idpath;
        this.ofmReceiverAddress = ofmrecvaddress;
        this.ofmAddressCache = ofmaddresscache;
        this.subscriberCouter = subscribercouter;
        this.scheduledExecutor = Executors.newScheduledThreadPool(scheduledExecutorCoreThread, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);  // as daemon thread
                return t;
            }
        });
    }

    /**
     * PubSub に用いるオーバレイの TransportIdPath を取得する
     * @return
     */
    public TransportIdPath getOFMPubSubIdPath() {
        return ofmPubSub;
    }

    /**
     * Openflow multicast 受信アドレスを取得する
     * @return
     */
    public IPMACPair getOFMReceiverAddress() {
        return ofmReceiverAddress;
    }

    /**
     * OFM アドレスキャッシュを取得する
     * @return
     */
    public OFMAddressCache getOFMAddressCache() {
        return ofmAddressCache;
    }

    /**
     * subscriber 数カウンタを取得する
     * ピア上の topic 
     * @return
     */
    public SubscriberCounter getSubscribedTopicCounter() {
        return subscriberCouter;
    }

    /**
     * 汎用 ScheduledExecutor を取得する
     * @return
     */
    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    /* (非 Javadoc)
     * @see org.piax.agent.impl.AgentHomeImpl#fin()
     */
    @Override
    public void fin() {
        super.fin();
        scheduledExecutor.shutdown();
        try {
            scheduledExecutor.awaitTermination(ExecutorShutdownAwaitingTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to stop ScheduledExecutor", e);
        }
        scheduledExecutor.shutdownNow();
    }
}
