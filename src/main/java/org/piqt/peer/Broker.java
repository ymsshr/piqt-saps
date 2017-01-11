/*
 * Broker.java - a broker implementation.
 * 
 * Copyright (c) 2016 National Institute of Information and Communications
 * Technology, Japan
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piqt.peer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.eclipse.moquette.server.Server;
import org.eclipse.moquette.server.config.ClasspathConfig;
import org.eclipse.moquette.server.config.IConfig;
import org.piax.agent.Agent;
import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerId;
import org.piax.common.TransportId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.gtrans.util.ChannelAddOnTransport;
import org.piax.samples.Util;
import org.piax.util.LocalInetAddrs;
import org.piqt.MqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.gate.messages.IPMACPair;
// shikata
import jp.piax.ofm.pubsub.PubSubManagerImpl;
import jp.piax.ofm.pubsub.PubSubManagerImplConfig;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;
import jp.piax.ofm.pubsub.misc.SubscriberCounter;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.agent.PubSubAgent;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;
import jp.piax.ofm.pubsub.piax.trans.TraceTransport;
import jp.piax.ofm.trans.OFMUdpLocator;


public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class
            .getPackage().getName());

    static int port = 1883;

    static Map<Thread, PeerMqEngineMoquette> engineMap = new HashMap<Thread, PeerMqEngineMoquette>();

    private IConfig config = null;
    private Properties properties = null;
    private Server mqttBroker = null;
    PeerMqEngineMoquette engine;
    
    //shikata 
    private PubSubManagerImpl pubSubManager;
    private String agentName;
    private PubSubAgentHomeImpl home = null;
    
    private Peer peer = engine.get
    
    // for createAgent
    private static final int DEFAULT_MY_LOCATORPORT = 12367;    // PIAX ポート番号
    private static final InetAddress DEFAULT_MY_LOCATORADDR = LocalInetAddrs.choice();
    private static final int DEFAULT_OFMPORT = 9888;        // Openflow multicast 送受信ポート
    private static final String DEFAULT_OFM_LOCATOR = "u" + DEFAULT_MY_LOCATORADDR.getHostAddress() + ":" + DEFAULT_OFMPORT;
    private static final long DEFAULT_OFMCACHE_TIMEOUT = 5*60*1000;     // OFM アドレスキャッシュのキャッシュ保持時間 (ms)

    private PubSubManagerImplConfig pubsubConfig;
    // config
    private String peer_locator = "12367";
    private String seed_locator = "12367";
    private String peer_name = "pubsub";
    
    public static final Class<? extends Agent> agent_class = PubSubAgent.class;
    private String ofm_locator = DEFAULT_OFM_LOCATOR;
    private String ofm_cachetimeout = Long.toString(DEFAULT_OFMCACHE_TIMEOUT);
    
    public Broker(PeerMqEngineMoquette engine, Properties web_config) throws AgentException {
        if (web_config == null) {
            config = new ClasspathConfig();
            config.setProperty("websocket_port", "");
            config.setProperty("port", String.valueOf(port));
            ++port;
        } else {
            properties = web_config;
            properties.setProperty("websocket_port", "");
        }
        System.setProperty("intercept.handler", "org.piqt.peer.Observer");
        this.engine = engine;
        
        // shikata createAgent
        pubsubConfig = new PubSubManagerImplConfig(
                Util.parseLocator(peer_locator),
                Util.parseLocator(seed_locator),
                peer_name,
                null,
                agent_class,
                new OFMUdpLocator((UdpLocator) Util.parseLocator(ofm_locator)),
                Long.parseLong(ofm_cachetimeout));
        
        // 各設定
     // OFM 受信アドレスに対応する MAC アドレス取得
        OFMUdpLocator ofmlocator = pubsubConfig.getOFMLocator();
        NetworkInterface ni = NetworkInterface
                .getByInetAddress(ofmlocator.getInetAddress());
        IPMACPair address = new IPMACPair(ofmlocator.getSocketAddress(),
                ni.getHardwareAddress());

        OFMAddressCache cache = new OFMAddressCache(
                pubsubConfig.getOFMCacheTimeout());

        SubscriberCounter subscribecounter = new SubscriberCounter();

        // setup instances
        peer = Peer.getInstance(peerId);

        transport = peer.newBaseChannelTransport(pubsubConfig.getPeerLocator()); // transport
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

        Set<AgentId> aids = home.getAgentIds();
        AgentId useragentid = null;
        for (AgentId aid : aids) {
            if (engine.getUserId().equals(home.getAgentName(aid))) {
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

            engine.creatPubSubAgent(home, pubSubManager.getPubSubManagerImplConfig(), useragentid, engine.getUserId());
        }
        
    }

    synchronized static void putEngine(PeerMqEngineMoquette e) {
        Thread th = Thread.currentThread();
        engineMap.put(th, e);
    }

    synchronized static PeerMqEngineMoquette getEngine() {
        return engineMap.get(Thread.currentThread());
    }

    synchronized static void removeEngine() {
        engineMap.remove(Thread.currentThread());
    }

    public void start() throws MqException {
        if (mqttBroker == null) {
            mqttBroker = new Server();
        }
        try {
            putEngine(engine);
            if (properties != null) {
                mqttBroker.startServer(properties);
            } else {
                mqttBroker.startServer(config);
            }
        } catch (IOException e) {
            logger.error("MQTT Broker not started: " + engine);
            throw new MqException(e);
        } finally {
            removeEngine();
        }
        logger.info("MQTT Broker started: " + engine);
    }

    public void stop() {
        logger.info("MQTT Broker Stopping: " + engine + " th = "
                + Thread.currentThread());
        if (mqttBroker != null) {
            mqttBroker.stopServer();
            mqttBroker = null;
        }
        logger.info("MQTT Broker stopped: " + engine);
    }

}
