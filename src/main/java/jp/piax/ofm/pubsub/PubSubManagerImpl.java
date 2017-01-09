package jp.piax.ofm.pubsub;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

//shikata for piqt.sh
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.misc.SubscriberCounter;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgentIf;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;
import jp.piax.ofm.pubsub.piax.trans.TraceTransport;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.mapdb.Engine;
import org.piax.agent.AgentConfigValues;
import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.Location;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.util.ChannelAddOnTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//shikata
import org.piqt.web.Launcher;
import org.piqt.peer.PeerMqEngine;
import org.piqt.peer.PeerMqEngineMoquette;

public class PubSubManagerImpl implements PubSubManager {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory
            .getLogger(PubSubManagerImpl.class);

    private PubSubManagerImplConfig config;
    private Peer peer;
    private ChannelTransport<?> transport;
    private Transport<OFMUdpLocator> ofmTransport;
    private MSkipGraph<Destination, ComparableKey<?>> skipgraph;
    private LLNet llnet;
    private DOLR<StringKey> dolr;
    private OFMPubSubOverlay ofmpubsub;
    private PubSubAgentHomeImpl home;

    private boolean active = false;
    private boolean started = false;

    private Map<String, UserPubSub> userPubSubs = new HashMap<String, UserPubSub>();

    // launcherを取得，launcherが持つengineを取得するため
    private Launcher launcher = Launcher.getInstance();
    private PeerMqEngineMoquette engine = launcher.getEngine();

    /**
     * コンストラクタ
     * 
     * @param config
     *            PubSubManagerImpl の設定
     */
    public PubSubManagerImpl(PubSubManagerImplConfig config) {
        if (config == null)
            throw new NullPointerException("config should not be null");

        this.config = config;
    }

    public PubSubAgentHomeImpl getPubSubAgentHome() {
        return home;
    }

    public PubSubManagerImplConfig getPubSubManagerImplConfig() {
        return config;
    }

    /*
     * (非 Javadoc)
     * 
     * @see jp.piaxinc.ofm.PubSubManager#start()
     */
    @Override
    public synchronized void start()
            throws IOException, IdConflictException, IllegalArgumentException,
            NoSuchOverlayException, IncompatibleTypeException {
        if (isActive())
            throw new IllegalStateException("This instance is already started");
        if (started)
            throw new IllegalStateException("Used instance");

        PeerId peerId = PeerId.newId();
        System.out.println("," + peerId);

        // OFM 受信アドレスに対応する MAC アドレス取得
        OFMUdpLocator ofmlocator = config.getOFMLocator();
        NetworkInterface ni = NetworkInterface
                .getByInetAddress(ofmlocator.getInetAddress());
        IPMACPair address = new IPMACPair(ofmlocator.getSocketAddress(),
                ni.getHardwareAddress());

        OFMAddressCache cache = new OFMAddressCache(
                config.getOFMCacheTimeout());

        SubscriberCounter subscribecounter = new SubscriberCounter();

        // setup instances
        peer = Peer.getInstance(peerId);

        transport = peer.newBaseChannelTransport(config.getPeerLocator()); // transport
                                                                           // for
                                                                           // overlay
        ofmTransport = peer.newBaseTransport(null, new TransportId("ofmudp"),
                ofmlocator); // transport for OFM

        // wrap for trace
        transport = new TraceTransport(transport, peerId);

        // setup overlays
        skipgraph = new MSkipGraph<Destination, ComparableKey<?>>(transport);
        llnet = new LLNet(skipgraph);
        dolr = new DOLR<StringKey>(skipgraph);
        ofmpubsub = new OFMPubSubOverlay(dolr, ofmTransport, cache);

        ChannelTransport<?> rpcTr = new ChannelAddOnTransport<PeerId>(
                skipgraph);

        // setup AgentHome for OFM pubsub
        home = new PubSubAgentHomeImpl(rpcTr, config.getAgClassPath(),
                ofmpubsub.getTransportIdPath(), address, cache,
                subscribecounter);

        // for location discovery
        // home.declareAttrib(AgentConfigValues.LOCATION_ATTRIB_NAME,
        // Location.class);
        // home.bindOverlay(AgentConfigValues.LOCATION_ATTRIB_NAME,
        // llnet.getTransportIdPath());

        // for OFGate discovery
        home.declareAttrib(CommonValues.OFGATE_ATTRIB, String.class);
        home.bindOverlay(CommonValues.OFGATE_ATTRIB, dolr.getTransportIdPath());

        logger.info("Peer ID       : {}", peer.getPeerId().toString());

        logger.info("Online peer");
        skipgraph.join(config.getSeedLocator());
        logger.info("Peer onlined");

        active = true;
        started = true;
        logger.info("Start PubSubManager");

        // shikata
        // piqt MQTT Broker Moquette 起動
        piqtShellScript();
        logger.info("Start PIQT MQTTBroker ");

    }

    /*
     * (非 Javadoc)
     * 
     * @see jp.piaxinc.ofm.PubSubManager#stop()
     */
    @Override
    public synchronized void stop() {
        if (home != null) {
            home.fin();
        }
        if (ofmpubsub != null) {
            ofmpubsub.fin();
        }
        if (dolr != null) {
            dolr.fin();
        }
        if (llnet != null) {
            llnet.fin();
        }
        if (skipgraph != null) {
            skipgraph.fin();
        }
        if (ofmTransport != null) {
            ofmTransport.fin();
        }
        if (transport != null) {
            transport.fin();
        }
        if (peer != null) {
            peer.fin();
        }
        active = false;
        logger.info("Finish PubSubManager");
    }

    public boolean isActive() {
        return active;
    }

    /*
     * (非 Javadoc)
     * 
     * @see jp.piaxinc.ofm.PubSubManager#getUserPubSub(java.lang.String)
     */
    @Override
    public synchronized UserPubSub getUserPubSub(String userid)
            throws AgentException {
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");

        Set<AgentId> aids = home.getAgentIds();
        AgentId useragentid = null;
        for (AgentId aid : aids) {
            if (userid.equals(home.getAgentName(aid))) {
                useragentid = aid;
                break;
            }
        }
        // engineで作成する仕様に shikata
        // engineを取得し，engine上にPubSubAgentを作成する．
        if (useragentid == null) {
            /*
             * useragentid = home.createAgent(config.getPubsubAgent(), userid);
             * PubSubAgentIf agent = home.getStub(useragentid);
             * agent.setUserId(userid);
             */

            // engine.creatPubSubAgent(home, config, useragentid, userid);
        }
        // sleep 状態の場合は復帰させる
        if (home.isAgentSleeping(useragentid)) {
            try {
                home.wakeupAgent(useragentid);
            } catch (ObjectStreamException e) {
                logger.error("Failed to wake up agent of {}", userid);
                logger.error("Failed to wake up agent", e);
                return null;
            }
        }

        if (!userPubSubs.containsKey(userid)) {
            UserPubSub userpubsub = new UserPubSubImpl(userid, useragentid,
                    home);
            userPubSubs.put(userid, userpubsub);
            return userpubsub;
        }
        return userPubSubs.get(userid);
    }

    // Toratani

    // shikata piqt.sh start
    public void piqtShellScript() {
        try {
            // String path = getPiqtshPath();
            Process process = new ProcessBuilder("sh",
                    "../../target/piqt-1.0.0-SNAPSHOT/piqt.sh").start();
            String text;
            InputStream is = process.getInputStream();
            InputStreamReader isr = new InputStreamReader(is, "UTF-8");
            BufferedReader reader = new BufferedReader(isr);
            StringBuilder builder = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                builder.append((char) c);
            }
            // 実行結果を格納
            text = builder.toString();
            int ret = process.waitFor();

            System.out.println(text);
            System.out.println(ret);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
     * shikata 没 public String getPiqtshPath(){ // classパスを取得 // jarならjarファイル名
     * String classPath = System.getProperty("PubSubManagerImpl.class.path");
     * File classFile = new File(classPath);
     * 
     * // 正規化する。 Path path = Paths.get(classFile.getAbsolutePath()); String
     * piqtHomePath = path.normalize().toString();
     * System.out.println(piqtHomePath);
     * 
     * return piqtHomePath; }
     * 
     ****/

    public PeerMqEngineMoquette getEngine() {
        return launcher.getEngine();
    }
}
