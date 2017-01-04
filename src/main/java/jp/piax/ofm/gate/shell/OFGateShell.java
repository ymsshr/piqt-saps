package jp.piax.ofm.gate.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import jp.piax.ofm.gate.OFMManager;
import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.gate.piax.agent.OFGate;
import jp.piax.ofm.gate.piax.agent.OFGateIf;
import jp.piax.ofm.pubsub.piax.trans.TraceTransport;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.piax.agent.AgentId;
import org.piax.agent.impl.AgentHomeImpl;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.TransportId;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.dolr.DOLR;
import org.piax.gtrans.sg.MSkipGraph;
import org.piax.gtrans.util.ChannelAddOnTransport;
import org.piax.samples.Util;
import org.piax.util.LocalInetAddrs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OFGateShell {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(OFGateShell.class);

    private static final String PROPERTY_FILE = "ofgateshell.properties";

    // 各種デフォルト値
    private static final int DEFAULT_MY_LOCATORPORT = 12367;    // PIAX ポート番号
    private static final InetAddress DEFAULT_MY_LOCATORADDR = LocalInetAddrs.choice();
    private static final String DEFAULT_PEER_LOCATOR = DEFAULT_MY_LOCATORADDR.getHostAddress() + ":" + DEFAULT_MY_LOCATORPORT;
    private static final int HTTP_CON_TIMEOUT = 500;   // OFM WebAPI 接続タイムアウト (ms)
    private static final int HTTP_REQ_TIMEOUT = 3000;   // OFM WebAPI 要求処理タイムアウト (ms)
    private static final String DEFAULT_PEER_NAME = "OFGate";
    private static final int DEFAULT_OFMPORT = 22222;    // Openflow multicast ポート番号
    private static final long DEFAULT_KEEPALIVEWAIT = 2*60*1000;   // Keep-alive メッセージ送信待機時間 (ms)


    // 各種動作時設定
    private PeerLocator peerLocator;
    private PeerLocator seedLocator;
    private String peerName = DEFAULT_PEER_NAME;
    private URI ofmControllerEndpoint;
    private int httpConnectTimeout = HTTP_CON_TIMEOUT;   // OFM WebAPI 接続タイムアウト (ms)
    private int httpRequestTimeout = HTTP_REQ_TIMEOUT;   // OFM WebAPI 要求処理タイムアウト (ms)
    private int ofmPort = DEFAULT_OFMPORT;
    private long keepaliveWaitTime = DEFAULT_KEEPALIVEWAIT;

    private Peer peer;
    private AgentHomeImpl home;
    private ChannelTransport<?> transport;
    private MSkipGraph<Destination, ComparableKey<?>> skipgraph;
    private DOLR<StringKey> dolr;
    private Transport<OFMUdpLocator> ofmTransport;

    private boolean active = false;

    private OFMManager manager;

    public static void main(String[] args) {
        final OFGateShell ss = new OFGateShell();

        if (!ss.initSetting(args)) {
            printUsage();
            return;
        }

        try {
            // 起動
            Date Time = new Date();
            System.out.println(Time.toLocaleString() +",OFgate起動");
            ss.start();

            // 終了処理登録
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        //if (ss.isActive()) {
                    		System.out.println("終了");
                            ss.stop();
                            logger.warn("Shell stopped in shutdown hook");
                        //}
                    } catch (Exception e) {
                        logger.error(e.getMessage(),e);
                    }
                }
            });

            
            // XXX ?
            synchronized(ss) {
                while (true) {
                  ss.wait();
                }
            }
            // コマンド受け付け処理
            //ss.cmdLoop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                // 停止
                ss.stop();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected boolean isActive() {
        return active;
    }

    /**
     * 設定の読み込みと内部パラメータのセットアップ
     * @param args コマンドライン引数
     * @return true 正常にセットアップ終了 false パラメータに異常あり
     */
    synchronized boolean initSetting(String[] args) {
        try {
            String propfile = PROPERTY_FILE;
            Properties serverprop = new Properties();

            String tmp_peer_locator = "";
            String tmp_seed_locator = "";
            String tmp_peer_name = "";
            String tmp_ofmport = "";
            String tmp_ofmc_endpoint = "";
            String tmp_http_con_timeout = "";
            String tmp_http_req_timeout = "";
            String tmp_ofm_keepaliveperiod = "";

            // Search 'p' option first.
            // If 'p' option is found, read property from given file.
            // If not, read default property file.
            for (int i = 0; i < args.length; i++) {
                if (args[i].charAt(0) == '-' && args[i].charAt(1) == 'p') {
                    if (2 < args[i].length() && '-' == args[i].charAt(2)) {
                        propfile = null;
                    } else {
                        i++;
                        if (i < args.length) {
                            propfile = args[i];
                        } else {
                            logger.error("-p option requires a property filename.");
                            return false;
                        }
                    }
                }
            }

            boolean is_fault = false;
            // サーバ設定ファイルの読み込み
            if (propfile != null) {
                File f = new File(propfile);
                boolean fileexist = f.exists() && f.isFile();

                if (!fileexist && !PROPERTY_FILE.equals(propfile)) {
                    logger.error("Property file does not found. : " + propfile);
                    return false;
                }

                if (fileexist) {
                    if (PROPERTY_FILE.equals(propfile))
                        logger.info("Found default property file." );

                    logger.info("Load from property file. : " + propfile);
                    // Try to read property file.
                    Reader freader = null;
                    try {
                        freader = new FileReader(propfile);
                        serverprop.load(freader);
                        // may
                        if (serverprop.containsKey("seed.locator")) {
                            tmp_seed_locator = serverprop.getProperty("seed.locator");
                        }
                        // may
                        if (serverprop.containsKey("peer.locator")) {
                            tmp_peer_locator = serverprop.getProperty("peer.locator");
                        }
                        // may
                        if (serverprop.containsKey("peer.name")) {
                            tmp_peer_name = serverprop.getProperty("peer.name");
                        }
                        // must
                        if (serverprop.containsKey("ofm.controller.endpoint")) {
                            tmp_ofmc_endpoint = serverprop.getProperty("ofm.controller.endpoint");
                        }
                        // may
                        if (serverprop.containsKey("ofm.controller.connect.timeout")) {
                            tmp_http_con_timeout = serverprop.getProperty("ofm.controller.connect.timeout");
                        }
                        // may
                        if (serverprop.containsKey("ofm.controller.request.timeout")) {
                            tmp_http_req_timeout = serverprop.getProperty("ofm.controller.request.timeout");
                        }
                        // may
                        if (serverprop.containsKey("ofm.port")) {
                            tmp_ofmport = serverprop.getProperty("ofm.port");
                        }
                        // may
                        if (serverprop.containsKey("ofm.subscriber.keepalive.period")) {
                            tmp_ofm_keepaliveperiod = serverprop.getProperty("ofm.subscriber.keepalive.period");
                        }
                    } catch (IOException e) {
                        logger.error("IO error at reading property file. : " + propfile);
                        is_fault = true;
                    } finally {
                        if (freader != null)
                            freader.close();
                    }
                }
            }

            // コマンドライン引数の処理
            for (int i = 0; i < args.length; i++) {
                String arg = args[i].trim();
                if (args[i].startsWith("-")) {
                    switch (arg.charAt(1)) {
                    case 'i':
                        i++;
                        if (i < args.length) {
                            tmp_peer_locator = args[i];
                        }
                        break;
                    case 's':
                        i++;
                        if (i < args.length) {
                            tmp_seed_locator = args[i];
                        }
                        break;
                    case 'n':
                        i++;
                        if (i < args.length) {
                            tmp_peer_name = args[i];
                        }
                        break;
                    case 'o':
                        i++;
                        if (i < args.length) {
                            tmp_ofmport = args[i];
                        }
                        break;
                    case '?':
                    case 'h':
                        return false;
                    default:
                        logger.error("Found an undefined option : " + args[i]);
                        return false;
                    }
                }
            }

            // Setup instance fields.
            if (tmp_peer_locator.isEmpty()) {
                logger.warn("Peer locator is not specified. Choose appropriate address.");
                tmp_peer_locator = DEFAULT_PEER_LOCATOR;
            }
            if (tmp_seed_locator.isEmpty()) {
                logger.warn("Seed locator is not specified. Use my peerlocator.");
                tmp_seed_locator = tmp_peer_locator;
            }
            if (tmp_peer_name.isEmpty()) {
                logger.warn("Peer name is not specified. Use default.");
                tmp_peer_name = DEFAULT_PEER_NAME;
            }
            if (tmp_ofmc_endpoint.isEmpty()) {
                logger.error("Openflow multicast controller API endpoint URL is required.");
                is_fault = true;
            }
            if (tmp_http_con_timeout.isEmpty()) {
                logger.warn("Connection timeout for Openflow multicast controller API is not specified. Use default.");
                tmp_http_con_timeout = Integer.toString(HTTP_CON_TIMEOUT);
            }
            if (tmp_http_req_timeout.isEmpty()) {
                logger.warn("Request timeout for Openflow multicast controller API is not specified. Use default.");
                tmp_http_req_timeout = Integer.toString(HTTP_REQ_TIMEOUT);
            }
            if (tmp_ofmport.isEmpty()) {
                logger.warn("Openflow multicast port is not specified. Use default.");
                tmp_ofmport = Integer.toString(DEFAULT_OFMPORT);
            }
            if (tmp_ofm_keepaliveperiod.isEmpty()) {
                logger.warn("Period time of sending keep alive to subscribers is not specified. Use default.");
                tmp_ofm_keepaliveperiod = Long.toString(DEFAULT_KEEPALIVEWAIT);
            }

            seedLocator = Util.parseLocator(tmp_seed_locator);
            peerLocator = Util.parseLocator(tmp_peer_locator);
            peerName = tmp_peer_name;
            ofmControllerEndpoint = URI.create(tmp_ofmc_endpoint);
            httpConnectTimeout = Integer.parseInt(tmp_http_con_timeout);
            httpRequestTimeout = Integer.parseInt(tmp_http_req_timeout);
            ofmPort = Integer.parseInt(tmp_ofmport);
            keepaliveWaitTime = Long.parseLong(tmp_ofm_keepaliveperiod);

            logger.info("Peer name                : {}", peerName);
            logger.info("Seed locator             : {} {}", seedLocator.getClass().getSimpleName(), seedLocator);
            logger.info("Peer locator             : {} {}", peerLocator.getClass().getSimpleName(), peerLocator);
            logger.info("OFM Contoller endpoint   : {}", ofmControllerEndpoint);
            logger.info("WebAPI timeout (connect) : {}", httpConnectTimeout);
            logger.info("WebAPI timeout (request) : {}", httpRequestTimeout);
            logger.info("Openflow multicast port  : {}", ofmPort);
            if (0 < keepaliveWaitTime) {
                logger.info("Keep-alive period        : {}", keepaliveWaitTime);
            } else {
                logger.info("Keep-alive message is desabled");
            }
            return !is_fault;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }

    }

    private static void printUsage() {
        System.out.println("Usage: SensorShell [options]");
        System.out.println("  -i <addr> sets Peer locator");
        System.out.println("  -s <addr> sets Seed locator");
        System.out.println("  -n <name> sets peer name");
        System.out.println("  -o <port> sets Openflow multicast port");
    }

    /**
     * 起動処理
     * @throws Exception
     */
    synchronized void start() throws Exception {
        if (isActive())
            throw new IllegalStateException("This instance is already started");

        PeerId peerId = PeerId.newId();

        // setup AgentPeer instance
        peer = Peer.getInstance(peerId);

        transport = peer.newBaseChannelTransport(peerLocator);
        ofmTransport = peer.newBaseTransport(null, new TransportId("ofmudp"), new OFMUdpLocator(new InetSocketAddress(ofmPort)));

        // wrap for trace
        transport = new TraceTransport(transport, peerId);

        skipgraph = new MSkipGraph<Destination, ComparableKey<?>>(transport);
        dolr = new DOLR<StringKey>(skipgraph);

        manager = new OFMManager(ofmControllerEndpoint, ofmTransport, 
                keepaliveWaitTime, httpConnectTimeout, httpRequestTimeout);

        ChannelTransport<?> rpcTr = new ChannelAddOnTransport<PeerId>(skipgraph);

        home = new AgentHomeImpl(rpcTr, new File("."));

        home.declareAttrib(CommonValues.OFGATE_ATTRIB, String.class);
        home.bindOverlay(CommonValues.OFGATE_ATTRIB, dolr.getTransportIdPath());

        // setup OFGate agent
        AgentId aid = home.createAgent(OFGate.class);
        logger.info("OFMGate Agent : {}", aid);
        OFGateIf ofmgate = home.getStub(aid);
        ofmgate.setOFGateManager(manager);

        logger.info("Peer ID       : {}", peer.getPeerId().toString());

        logger.info("Online peer");
        skipgraph.join(seedLocator);
        logger.info("Peer onlined");

        active = true;
    }

    /**
     * 終了処理
     * @throws Exception
     */
    synchronized void stop() throws Exception {
        if (home != null) {
            home.fin();
        }
        if (ofmTransport != null) {
            ofmTransport.fin();
        }
        if (dolr != null) {
            dolr.fin();
        }
        if (skipgraph != null) {
            skipgraph.fin();
        }
        if (transport != null) {
            transport.fin();
        }
        if (peer != null) {
            peer.fin();
        }
        logger.debug("Fin:"+this.peerName);

        active = false;
    }

    /**
     * コマンド入力待ち
     */
    synchronized void cmdLoop() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                System.out.print("Input Command >");
                String line = reader.readLine();

                if (line == null)   // CTRL+C
                    break;

                if (line.trim().isEmpty())
                    continue;

                String[] cmds = line.split("\\s+");

                if ("bye".equals(cmds[0])) {
                    // bye : 終了
                    break;
                } else if ("show".equals(cmds[0])) {
                    Set<OFMManager.OFMTableEntry> table = manager.getOFMTable();
                    System.out.println("- OFM Manager table -");
                    if (table == null || table.isEmpty()) {
                        System.out.println(" No topic");
                    } else {
                        StringBuilder buf = new StringBuilder();
                        for (OFMManager.OFMTableEntry ele : table) {
                            buf.append(" Topic:["+ele.getTopic()+"]");
                            buf.append(System.lineSeparator());
                            buf.append("  OFM address : " + ele.getOFMAddress());
                            buf.append(System.lineSeparator());
                            buf.append("  OFM subscribers : ");
                            for (IPMACPair ofmrecvaddr : ele.getSubscribersOFMAddress()) {
                                buf.append(" ").append(ofmrecvaddr.toString());
                            }
                            buf.append(System.lineSeparator()).append(System.lineSeparator());
                        }
                        System.out.println(buf.toString());
                    }
                    System.out.println("---------------------");
                    System.out.println();
                } else {
                    System.out.println("-- Commands --");
                    System.out.println("bye                   Exit");
                    System.out.println("show                  Show topic and OFM table");
                    System.out.println("--------------");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
