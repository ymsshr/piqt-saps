package jp.piax.ofm.pubsub.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.Date;
import java.util.Properties;

import jp.piax.ofm.pubsub.PubSubManager;
import jp.piax.ofm.pubsub.PubSubManagerImpl;
import jp.piax.ofm.pubsub.PubSubManagerImplConfig;
import jp.piax.ofm.pubsub.UserPubSub;
import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;
import jp.piax.ofm.pubsub.common.PubSubAgentConfigValues;
import jp.piax.ofm.pubsub.common.YosPubSubAgentConfigValues;
import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.YosSimpleMonitor;
import jp.piax.ofm.pubsub.piax.agent.YosPubSubAgent;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;
import jp.piax.ofm.pubsub.web.Authenticator;
import jp.piax.ofm.pubsub.web.DummyAuthenticator;
import jp.piax.ofm.pubsub.web.WebInterfaceService;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.piax.agent.Agent;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.samples.Util;
import org.piax.util.LocalInetAddrs;
import org.piqt.web.Launcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.piqt.web.Launcher;

public class Shell {
    private static final Logger logger = LoggerFactory.getLogger(Shell.class);

    private static final String PROPERTY_FILE = "pubsubshell.properties";

    // 各種デフォルト値
    private static final int DEFAULT_MY_LOCATORPORT = 12367;    // PIAX ポート番号
    private static final InetAddress DEFAULT_MY_LOCATORADDR = LocalInetAddrs.choice();
    private static final String DEFAULT_PEER_LOCATOR = DEFAULT_MY_LOCATORADDR.getHostAddress() + ":" + DEFAULT_MY_LOCATORPORT;
    private static final String DEFAULT_PEER_NAME = "PubSub";

    private static final int DEFAULT_WEB_PORT = 8888;           // Jetty listen ポート番号
    private static final String DEFAULT_DOCUMENT_ROOT = "../web";    // Document root
    private static final Class<? extends Authenticator> DEFAULT_AUTHENTICATOR = DummyAuthenticator.class;
    private static final String DEFAULT_LOGINPAGE = "/login.html";    // ログインページ
    private static final String DEFAULT_USERPAGE = "/";      // ログイン後の初期ページ
    private static final int DEFAULT_SESSIONTIMEOUT = 15*60;   // セッション有効期間 (sec)

    private static final int DEFAULT_OFMPORT = 9888;        // Openflow multicast 送受信ポート
    private static final String DEFAULT_OFM_LOCATOR = "u" + DEFAULT_MY_LOCATORADDR.getHostAddress() + ":" + DEFAULT_OFMPORT;
    private static final long DEFAULT_OFMCACHE_TIMEOUT = 5*60*1000;     // OFM アドレスキャッシュのキャッシュ保持時間 (ms)

    // 各種動作時設定
    private PubSubManagerImplConfig pubsubConfig;

    private int webPort = DEFAULT_WEB_PORT;
    private String documentRoot = DEFAULT_DOCUMENT_ROOT;
    private Authenticator authenticator = null;
    private String loginPage = DEFAULT_LOGINPAGE;
    private String userPage = DEFAULT_USERPAGE;
    private int sessionTimeout = DEFAULT_SESSIONTIMEOUT;

    private WebInterfaceService webService = null;
    private PubSubManager pubSubManager = null;
    private boolean active = false;
    
    private Launcher lancher;


    public static void main(String[] args) {
        final Shell ss = new Shell();

        if (!ss.initSetting(args)) {
            printUsage();
            return;
        }
        
        
        
        
        

        try {
            // 起動
            Date Time = new Date();
            System.out.println(Time.toLocaleString() +",broker起動");
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
    @SuppressWarnings("unchecked")
    synchronized boolean initSetting(String[] args) {
        try {
            String propfile = PROPERTY_FILE;
            Properties serverprop = new Properties();

            String tmp_peer_locator = "";
            String tmp_seed_locator = "";
            String tmp_peer_name = "";

            String tmp_web_port = "";
            String tmp_document_root = "";
            String tmp_login_page = "";
            String tmp_user_page = "";
            String tmp_session_timeout = "";
            String tmp_authenticator = "";

            String tmp_ofm_locator = "";
            String tmp_ofm_cachetimeout = "";
            String tmp_ofm_countertimewindow = "";
            String tmp_ofm_pollingperiod = "";
            String tmp_ofm_duplicatedmessagequeue = "";
            String tmp_ofm_loopback_timeout = "";
            String tmp_ofm_gc_period = "";
            String tmp_ofm_usecachebeforepublish = "";
            String tmp_monitorclass = "";
            String tmp_agentclass = "";

            String tmp_yos_migrate_delay_base = "";
            String tmp_yos_migrate_delay_width = "";

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
                        // may
                        if (serverprop.containsKey("web.port")) {
                            tmp_web_port = serverprop.getProperty("web.port");
                        }
                        // may
                        if (serverprop.containsKey("web.documentroot")) {
                            tmp_document_root = serverprop.getProperty("web.documentroot");
                        }
                        // may
                        if (serverprop.containsKey("web.url.login")) {
                            tmp_login_page = serverprop.getProperty("web.url.login");
                        }
                        // may
                        if (serverprop.containsKey("web.url.start")) {
                            tmp_user_page = serverprop.getProperty("web.url.start");
                        }
                        // may
                        if (serverprop.containsKey("web.session.timeout")) {
                            tmp_session_timeout = serverprop.getProperty("web.session.timeout");
                        }
                        // may
                        if (serverprop.containsKey("web.session.authenticator.classname")) {
                            tmp_authenticator = serverprop.getProperty("web.session.authenticator.classname");
                        }
                        // may
                        if (serverprop.containsKey("ofm.locator")) {
                            tmp_ofm_locator = serverprop.getProperty("ofm.locator");
                        }
                        // may
                        if (serverprop.containsKey("ofm.cache.timeout")) {
                            tmp_ofm_cachetimeout = serverprop.getProperty("ofm.cache.timeout");
                        }
                        // may
                        if (serverprop.containsKey("ofm.counter.timewindow")) {
                            tmp_ofm_countertimewindow = serverprop.getProperty("ofm.counter.timewindow");
                        }
                        // may
                        if (serverprop.containsKey("ofm.monitor.pollingperiod")) {
                            tmp_ofm_pollingperiod = serverprop.getProperty("ofm.monitor.pollingperiod");
                        }
                        // may
                        if (serverprop.containsKey("ofm.duplicatedmessagequeuelength")) {
                            tmp_ofm_duplicatedmessagequeue = serverprop.getProperty("ofm.duplicatedmessagequeuelength");
                        }
                        // may
                        if (serverprop.containsKey("ofm.loopbacktimeout")) {
                            tmp_ofm_loopback_timeout = serverprop.getProperty("ofm.loopbacktimeout");
                        }
                        // may
                        if (serverprop.containsKey("ofm.gcperiod")) {
                            tmp_ofm_gc_period = serverprop.getProperty("ofm.gcperiod");
                        }
                        // may
                        if (serverprop.containsKey("ofm.cache.usebeforepublish")) {
                            tmp_ofm_usecachebeforepublish = serverprop.getProperty("ofm.cache.usebeforepublish");
                        }
                        // may
                        if (serverprop.containsKey("ofm.monitor.classname")) {
                            tmp_monitorclass = serverprop.getProperty("ofm.monitor.classname");
                        }
                        // may
                        if (serverprop.containsKey("ofm.pubsubagent.classname")) {
                            tmp_agentclass = serverprop.getProperty("ofm.pubsubagent.classname");
                        }
                        // may
                        if (serverprop.containsKey("yos.ofm.migration.delay.base")) {
                            tmp_yos_migrate_delay_base = serverprop.getProperty("yos.ofm.migration.delay.base");
                        }
                        // may
                        if (serverprop.containsKey("yos.ofm.migration.delay.width")) {
                            tmp_yos_migrate_delay_width = serverprop.getProperty("yos.ofm.migration.delay.width");
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
                    case 'w':
                        i++;
                        if (i < args.length) {
                            tmp_web_port = args[i];
                        }
                        break;
                    case 'o':
                        i++;
                        if (i < args.length) {
                            tmp_ofm_locator = args[i];
                        }
                        break;
                    case 'y':
                        tmp_monitorclass = YosSimpleMonitor.class.getCanonicalName();
                        tmp_agentclass = YosPubSubAgent.class.getCanonicalName();
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
            if (tmp_web_port.isEmpty()) {
                logger.warn("Web port is not specified. Use default.");
                tmp_web_port = Integer.toString(DEFAULT_WEB_PORT);
            }
            if (tmp_document_root.isEmpty()) {
                logger.warn("Document root is not specified. Use default.");
                tmp_document_root = DEFAULT_DOCUMENT_ROOT;
            }
            if (tmp_login_page.isEmpty()) {
                logger.warn("Login page is not specified. Use default.");
                tmp_login_page = DEFAULT_LOGINPAGE;
            }
            if (tmp_user_page.isEmpty()) {
                logger.warn("User page is not specified. Use default.");
                tmp_user_page = DEFAULT_USERPAGE;
            }
            if (tmp_session_timeout.isEmpty()) {
                logger.warn("Session timeout is not specified. Use default.");
                tmp_session_timeout = Integer.toString(DEFAULT_SESSIONTIMEOUT);
            }
            if (tmp_authenticator.isEmpty()) {
                logger.warn("Login authenticator is not specified. Use default.");
                tmp_authenticator = DEFAULT_AUTHENTICATOR.getCanonicalName();
            }
            if (tmp_ofm_locator.isEmpty()) {
                logger.warn("Locator for openflow multicast is not specified. Use default.");
                tmp_ofm_locator = DEFAULT_OFM_LOCATOR;
            }
            if (!tmp_ofm_locator.startsWith("u")) {
                if (tmp_ofm_locator.startsWith("t") || tmp_ofm_locator.startsWith("e")) {
                    logger.error("Locator for openflow multicast should be UdpLocator (start with \"u\").");
                    is_fault = true;
                } else {
                    tmp_ofm_locator = "u" + tmp_ofm_locator;
                }
            }
            if (tmp_ofm_cachetimeout.isEmpty()) {
                logger.warn("Timeout for OFM address cache on subscriber is not specified. Use default.");
                tmp_ofm_cachetimeout = Long.toString(DEFAULT_OFMCACHE_TIMEOUT);
            }
            if (tmp_ofm_countertimewindow.isEmpty()) {
                logger.warn("Time window for calculating traffic statistics on subscriber is not specified. Use default.");
                tmp_ofm_countertimewindow = Long.toString(PubSubAgentConfigValues.DEFAULT_COUNTER_TIMEWINDOW);
            }
            if (tmp_ofm_pollingperiod.isEmpty()) {
                logger.warn("Polling period of pub/sub monitor is not specified. Use default.");
                tmp_ofm_pollingperiod = Long.toString(PubSubAgentConfigValues.DEFAULT_POLLINGPERIOD);
            }
            if (tmp_ofm_duplicatedmessagequeue.isEmpty()) {
                logger.warn("Length of duplicated message detection queue is not specified. Use default.");
                tmp_ofm_duplicatedmessagequeue = Integer.toString(PubSubAgentConfigValues.DEFAULT_DUPLICATED_MESSAGE_QUEUE_LENGTH);
            }
            if (tmp_ofm_loopback_timeout.isEmpty()) {
                logger.warn("OFM loopback timeout is not specified. Use default.");
                tmp_ofm_loopback_timeout = Long.toString(PubSubAgentConfigValues.DEFAULT_RECV_LOOPBACK_TIMEOUT);
            }
            if (tmp_ofm_gc_period.isEmpty()) {
                logger.warn("OFM gabege collection period is not specified. Use default.");
                tmp_ofm_gc_period = Long.toString(OFMPubSubOverlay.DEFAULT_GC_PERIOD);
            }
            tmp_ofm_usecachebeforepublish = tmp_ofm_usecachebeforepublish.trim().toLowerCase(); // format
            if (tmp_ofm_usecachebeforepublish.isEmpty()) {
                logger.warn("A flag for cache before publish is not specified. Use default.");
                tmp_ofm_usecachebeforepublish = Boolean.toString(PubSubAgentConfigValues.DEFAULT_USE_OFMCACHE_BEFORE_PUBLISH);
            }
            if ("1".equals(tmp_ofm_usecachebeforepublish) || tmp_ofm_usecachebeforepublish.startsWith("t")) {
                tmp_ofm_usecachebeforepublish = Boolean.TRUE.toString();
            }
            if (tmp_monitorclass.isEmpty()) {
                logger.warn("Class name of pub/sub monitor is not specified. Use default.");
                tmp_monitorclass = PubSubAgentConfigValues.DEFAULT_MONITORCLASS.getCanonicalName();
            }
            if (tmp_agentclass.isEmpty()) {
                logger.warn("Class name of pub/sub agent is not specified. Use default.");
                tmp_agentclass = PubSubManagerImplConfig.DEFAULT_PUBSUB_AGENTCLASS.getCanonicalName();
            }
            Class<? extends Agent> agent_class = (Class<? extends Agent>) Class.forName(tmp_agentclass);
            if (tmp_yos_migrate_delay_base.isEmpty()) {
                logger.warn("The base delay time to migrate OFM topic in YosPubSubAgent is not specified. Use default.");
                tmp_yos_migrate_delay_base = Long.toString(YosPubSubAgentConfigValues.DEFAULT_OFM_MIGRATION_DELAY_BASE);
            }
            if (tmp_yos_migrate_delay_width.isEmpty()) {
                logger.warn("The additional delay width to migrate OFM topic in YosPubSubAgent is not specified. Use default.");
                tmp_yos_migrate_delay_width = Long.toString(YosPubSubAgentConfigValues.DEFAULT_OFM_MIGRATION_DELAY_WIDTH);
            }

            pubsubConfig = new PubSubManagerImplConfig(
                Util.parseLocator(tmp_peer_locator),
                Util.parseLocator(tmp_seed_locator),
                tmp_peer_name,
                null,
                agent_class,
                new OFMUdpLocator((UdpLocator) Util.parseLocator(tmp_ofm_locator)),
                Long.parseLong(tmp_ofm_cachetimeout));

            PubSubAgentConfigValues.SubscriberPollingPeriod = Long.parseLong(tmp_ofm_pollingperiod);
            PubSubAgentConfigValues.TrafficCounterTimewindow = Long.parseLong(tmp_ofm_countertimewindow);
            PubSubAgentConfigValues.DuplicatedMessageQueueLength = Integer.parseInt(tmp_ofm_duplicatedmessagequeue);
            PubSubAgentConfigValues.UseOFMCacheBeforePublish = Boolean.parseBoolean(tmp_ofm_usecachebeforepublish);
            PubSubAgentConfigValues.RecvLoopbackTimeout = Long.parseLong(tmp_ofm_loopback_timeout);
            OFMPubSubOverlay.LoopbackTimeout = PubSubAgentConfigValues.RecvLoopbackTimeout;     // loopbackタイムアウトの設定は共有する
            OFMPubSubOverlay.GcPeriod = Long.parseLong(tmp_ofm_gc_period);
            PubSubAgentConfigValues.MonitorClazz = (Class<? extends PubSubMonitor>) Class.forName(tmp_monitorclass);
            YosPubSubAgentConfigValues.OFMtopicMigrationDelayBase = Long.parseLong(tmp_yos_migrate_delay_base);
            YosPubSubAgentConfigValues.OFMtopicMigrationDelayAdditionalWidth = Long.parseLong(tmp_yos_migrate_delay_width);

            webPort = Integer.parseInt(tmp_web_port);
            documentRoot = tmp_document_root;
            loginPage = tmp_login_page;
            userPage = tmp_user_page;
            sessionTimeout = Integer.parseInt(tmp_session_timeout);

            Class<? extends Authenticator> authenticator_class = (Class<? extends Authenticator>) Class.forName(tmp_authenticator);
            Constructor<? extends Authenticator> auth = authenticator_class.getConstructor(Properties.class);
            authenticator = auth.newInstance(serverprop);

            logger.info("Peer name                          : {}", pubsubConfig.getPeerName());
            logger.info("Seed locator                       : {} {}", pubsubConfig.getSeedLocator().getClass().getSimpleName(), pubsubConfig.getSeedLocator());
            logger.info("Peer locator                       : {} {}", pubsubConfig.getPeerLocator().getClass().getSimpleName(), pubsubConfig.getPeerLocator());

            logger.info("Web port                           : {}", webPort);
            logger.info("Document root                      : {}", documentRoot);
            logger.info("Login page                         : {}", loginPage);
            logger.info("User page                          : {}", userPage);
            logger.info("Session timeout (s)                : {}", sessionTimeout);
            logger.info("Authenticator                      : {}", authenticator_class.getSimpleName());
            logger.info("Publish/Subscribe Agent            : {}", agent_class.getSimpleName());
            logger.info("OFM receiver address               : {}", pubsubConfig.getOFMLocator());
            logger.info("OFM cache timeout (ms)             : {}", pubsubConfig.getOFMCacheTimeout());
            logger.info("OFM stat timewindow (ms)           : {}", PubSubAgentConfigValues.TrafficCounterTimewindow);
            logger.info("OFM polling period (ms)            : {}", PubSubAgentConfigValues.SubscriberPollingPeriod);
            logger.info("Duplicated msg queue length        : {}", PubSubAgentConfigValues.DuplicatedMessageQueueLength);
            logger.info("OFM loopback timeout (ms)          : {}", PubSubAgentConfigValues.RecvLoopbackTimeout);
            logger.info("OFM garbage collection period (ms) : {}", OFMPubSubOverlay.GcPeriod);
            logger.info("Use OFM cache before publish       : {}", PubSubAgentConfigValues.UseOFMCacheBeforePublish);
            logger.info("OFM monitor                        : {}", PubSubAgentConfigValues.MonitorClazz.getSimpleName());
            if (agent_class == YosPubSubAgent.class) {
                logger.info("[Yos] Base delay time to be OFM (ms)        : {}", YosPubSubAgentConfigValues.OFMtopicMigrationDelayBase);
                logger.info("[Yos] Additional delay width to be OFM (ms) : {}", YosPubSubAgentConfigValues.OFMtopicMigrationDelayAdditionalWidth);
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
        System.out.println("  -w <port> sets web port");
        System.out.println("  -o <addr> sets Openflow multicast port");
        System.out.println("  -y        uses YosPubSubAgent");
    }

    /**
     * 起動処理
     * @throws Exception
     */
    synchronized void start() throws Exception {
        pubSubManager = new PubSubManagerImpl(pubsubConfig);
        WebUserAdaptorManager adaptor_manager = new WebUserAdaptorManager(pubSubManager);
        

        webService = new WebInterfaceService(webPort, documentRoot, loginPage, userPage, sessionTimeout, authenticator, adaptor_manager);

        pubSubManager.start();
        webService.start();
        

        active = true;
        
    }

    /**
     * 終了処理
     * @throws Exception
     */
    synchronized void stop() throws Exception {
        if (webService != null) {
            webService.stop();
        }
        if (pubSubManager != null) {
            pubSubManager.stop();
        }
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
                } else if ("s".equals(cmds[0])) {
                    if (cmds.length != 3) {
                        System.out.println("s (userid) (topic)        Subscribe (topic) by (userid)");
                        continue;
                    }
                    String userid = cmds[1];
                    String topic = cmds[2];
                    UserPubSub ps = pubSubManager.getUserPubSub(userid);
                    ps.subscribe(topic);
                } else if ("u".equals(cmds[0])) {
                    if (cmds.length != 3) {
                        System.out.println("u (userid) (topic)        Unsubscribe (topic) by (userid)");
                        continue;
                    }
                    String userid = cmds[1];
                    String topic = cmds[2];
                    UserPubSub ps = pubSubManager.getUserPubSub(userid);
                    ps.unsubscribe(topic);
                } else if ("p".equals(cmds[0])) {
                    if (cmds.length != 4) {
                        System.out.println("p (userid) (topic) (msg)  Publish (msg) on (topic) by (userid)");
                        continue;
                    }
                    String userid = cmds[1];
                    String topic = cmds[2];
                    String msg = cmds[3];
                    UserPubSub ps = pubSubManager.getUserPubSub(userid);
                    ps.publish(topic, msg);
                } else {
                    System.out.println("-- Commands --");
                    System.out.println("s (userid) (topic)        Subscribe (topic) by (userid)");
                    System.out.println("u (userid) (topic)        Unsubscribe (topic) by (userid)");
                    System.out.println("p (userid) (topic) (msg)  Publish (msg) on (topic) by (userid)");
                    System.out.println("bye                       Exit");
                    System.out.println("--------------");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
