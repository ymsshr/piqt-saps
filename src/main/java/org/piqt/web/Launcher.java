/*
 * Launcher.java - The startup class
 * 
 * Copyright (c) 2016 National Institute of Information and Communications
 * Technology, Japan
 *
 * You can redistribute it and/or modify it under either the terms of
 * the AGPLv3 or PIAX binary code license. See the file COPYING
 * included in the PIQT package for more in detail.
 */
package org.piqt.web;

import static org.piqt.peer.Util.isEmpty;

import static org.piqt.peer.Util.newline;
import static org.piqt.peer.Util.stackTraceStr;
import static org.piqt.web.MqttPiaxConfig.KEY_AUTO_START;
import static org.piqt.web.MqttPiaxConfig.KEY_CLEAR_START;
import static org.piqt.web.MqttPiaxConfig.KEY_JETTY_HOST;
import static org.piqt.web.MqttPiaxConfig.KEY_JETTY_PORT;
import static org.piqt.web.MqttPiaxConfig.KEY_MQTT_PERSISTENT_STORE;
import static org.piqt.web.MqttPiaxConfig.KEY_PIAX_DOMAIN_NAME;
import static org.piqt.web.MqttPiaxConfig.KEY_PIAX_IP_ADDRESS;
import static org.piqt.web.MqttPiaxConfig.KEY_PIAX_PORT;
import static org.piqt.web.MqttPiaxConfig.KEY_PIAX_SEED_IP_ADDRESS;
import static org.piqt.web.MqttPiaxConfig.KEY_PIAX_SEED_PORT;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import net.arnx.jsonic.JSON;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.piax.agent.AgentException;
import org.piax.common.ComparableKey;
import org.piax.common.Destination;
import org.piax.common.PeerId;
import org.piax.common.PeerLocator;
import org.piax.common.wrapper.StringKey;
import org.piax.gtrans.ChannelTransport;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.Peer;
import org.piax.gtrans.Transport;
import org.piax.gtrans.ov.dolr.DOLR;
import org.piax.gtrans.ov.llnet.LLNet;
import org.piax.gtrans.ov.sg.MSkipGraph;
import org.piax.gtrans.ov.szk.Suzaku;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.piax.samples.Util;
import org.piax.util.LocalInetAddrs;
import org.piqt.MqCallback;
import org.piqt.MqDeliveryToken;
import org.piqt.MqException;
import org.piqt.MqMessage;
import org.piqt.MqTopic;
import org.piqt.peer.ClusterId;
import org.piqt.peer.LATKey;
import org.piqt.peer.PeerMqDeliveryToken;
import org.piqt.peer.PeerMqEngineMoquette;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.PubSubManager;
import jp.piax.ofm.pubsub.PubSubManagerImplConfig;
import jp.piax.ofm.pubsub.UserPubSub;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;
import jp.piax.ofm.pubsub.piax.trans.OFMPubSubOverlay;
import jp.piax.ofm.pubsub.web.Authenticator;
import jp.piax.ofm.pubsub.web.DummyAuthenticator;
import jp.piax.ofm.pubsub.web.WebInterfaceService;
import jp.piax.ofm.trans.OFMUdpLocator;





public class Launcher {

    private static final Logger logger = LoggerFactory.getLogger(Launcher.class
            .getPackage().getName());
    private static Launcher instance;
    private static boolean cont = true;
    
    
    public static enum response {
        OK, NG
    };

    public static String PROPERTY_FILE_NAME = "config/piqt.properties";
    public static String LOG_FILE_NAME = "piqt.log";
    public static String PIQT_PATH_PROPERTY_NAME = "piqt.path";

    Server server;
    MqttPiaxConfig config;

    PeerLocator loc;
    Peer peer;
    PeerMqEngineMoquette e;
    ChannelTransport<UdpLocator> c;
    ClusterId cid;
    Suzaku<Destination, LATKey> szk;

    long startDate;
    
    // pubsubManager shikata
    private PubSubManagerImplConfig pubsubconfig;
    //private Peer peer;
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

    ////

    public static String version() {
        return Launcher.class.getPackage().getImplementationVersion();
    }

    public synchronized static Launcher getInstance() {
        if (instance == null) {
            instance = new Launcher();
        }
        return instance;
    }

    class Jdata {
        public String msg;
        public String detail;

        Jdata(String m, String d) {
            msg = m;
            detail = d;
        }
    }

    private Response buildResponse(response status, String msg, String detail) {
        ResponseBuilder b;

        if (status == response.OK) {
            b = Response.ok();
        } else {
            b = Response.serverError();
        }
        b.type(MediaType.APPLICATION_JSON);
        b.entity(JSON.encode(new Jdata(msg, detail)));
        return b.build();

    }

    private void readConfig(File configFile) throws IOException {
        if (config == null) {
            InputStream is = new FileInputStream(configFile);
            MqttOnPiaxApp.configuration = new Properties();
            MqttOnPiaxApp.configuration.load(is);
            is.close();
            config = new MqttPiaxConfig(MqttOnPiaxApp.configuration);
        }
    }

    /* XXX not used?
    private String escapeSeparators(String str) {
        return str.replace("\\", "\\\\").replace("#", "\\#")
                .replace("!", "\\!").replace("=", "\\=").replace(":", "\\:");
    } */

    private boolean parseCommandLine(String args[]) {
        if (args.length == 1 && args[0].equals("-version")) {
            System.out.println(version());
            return false;
        }
        return true;
    }

    private void checkDomain() {
        String domain = (String) config.get(KEY_PIAX_DOMAIN_NAME);
        if (isEmpty(domain)) {
            // The default cluster ID is empty string.
            domain = "";
            config.setConfig(KEY_PIAX_DOMAIN_NAME, domain);
            logger.info("domain name is set as " + (isEmpty(domain) ? "default" : domain));
            MqttOnPiaxApp.configuration.setProperty(KEY_PIAX_DOMAIN_NAME,
                    domain);
        }
/*            FileOutputStream os;
            File conf = configFile();
            try {
                os = new FileOutputStream(conf);
                MqttOnPiaxApp.configuration.store(os, "update props");
                os.close();
            } catch (IOException e1) {
                String msg = "Failed to store config in " + conf.getPath() + ".";
                String detail = stackTraceStr(e1);
                logger.error(msg + newline + detail);
            }
        }*/
    }
    
    private File configFile() {
        return new File(System.getProperty(PIQT_PATH_PROPERTY_NAME), PROPERTY_FILE_NAME);
    }

    private void init(String args[]) throws AgentException {
        server = null;

        if (!parseCommandLine(args)) {
            System.exit(1);
        }

        MqttOnPiaxApp.setLauncher(this);

        try {
            readConfig(configFile());
        } catch (IOException e1) {
            String msg = "Failed to read configuration file: ";
            String detail = stackTraceStr(e1);
            logger.error(msg + detail);
            System.exit(1);
        }

        checkDomain();
        /*try {
            setLogger();
        } catch (SecurityException | IOException e1) {
            String msg = "Failed to setLogger().";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            System.exit(1);
        }*/

        server = new Server();
        ServerConnector http = new ServerConnector(server);
        String jh = (String) config.get(KEY_JETTY_HOST);
        int jp = (int) config.get(KEY_JETTY_PORT);
        if (isEmpty(jh) || jp == 0) {
            logger.error("Wrong jetty host or jetty port.");
            System.exit(1);
        }
        http.setHost(jh);
        http.setPort(jp);
        http.setIdleTimeout(30000);

        server.addConnector(http);

        /**/
        ProtectionDomain protectionDomain = Launcher.class
                .getProtectionDomain();
        URL location = protectionDomain.getCodeSource().getLocation();
        logger.debug("protectionDomain=" + location.toExternalForm());

        WebAppContext wap = new WebAppContext();
        wap.setContextPath("/");
        wap.setParentLoaderPriority(false);

        String rb = System.getProperty(PIQT_PATH_PROPERTY_NAME) + File.separator + "WebContent";
        logger.info("content path: " + rb);
        wap.setResourceBase(rb);

        server.setHandler(wap);
        /**/

        try {
            server.start();
        } catch (Exception e1) {
            String msg = "Failed to setLogger().";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            System.exit(1);
        }

        startDate = 0;

        loc = null;
        peer = null;
        cid = null;
        c = null;
        szk = null;
        e = null;

        if (config.get(KEY_AUTO_START).equals("yes")) {
            Response res = this.start();
            if (res.getStatusInfo() != Response.Status.OK) {
                logger.error((String) res.getEntity());
            }
        }
    }
        

    private void fin() {
        if (szk != null) {
            szk.fin();
            szk = null;
        }
        if (c != null) {
            c.fin();
            c = null;
        }
        if (peer != null) {
            peer.fin();
            peer = null;
        }
    }
/*
    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Exception e) {
            String msg = "Failed to getHostName().";
            String detail = stackTraceStr(e);
            logger.error(msg + newline + detail);
        }
        return "UnknownHost";
    }
*/
    public Response start() throws AgentException {
        String startLog = "";
        if (startDate != 0) {
            String msg = "MQTT already started.";
            logger.info(msg);
            return buildResponse(response.OK, msg, String.valueOf(startDate));
        }

        if (config.get(KEY_CLEAR_START).equals("yes")) {
            String store = (String) config.get(KEY_MQTT_PERSISTENT_STORE);
            File f = new File(store);
            if (f.exists()) {
                f.delete();
                logger.info("persistent store " + store + " removed.");
            } else {
                logger.info("persistent store " + store + " does not exist.");
            }
        }

        PeerMqDeliveryToken.USE_DELEGATE = true;
        /* NodeMonitor.PING_TIMEOUT = 1000000; // to test the retrans without ddll
                                            // fix
        RQManager.RQ_FLUSH_PERIOD = 50; // the period for flushing partial
                                        // results in intermediate nodes
        RQManager.RQ_EXPIRATION_GRACE = 80; // additional grace time before
                                            // removing RQReturn in intermediate
                                            // nodes
        RQManager.RQ_RETRANS_PERIOD = 1000; // range query retransmission period
        MessagingFramework.ACK_TIMEOUT_THRES = 2000;
        MessagingFramework.ACK_TIMEOUT_TIMER = MessagingFramework.ACK_TIMEOUT_THRES + 50; */

        boolean bad_param = false;
        String msg = "";
        String pip = (String) config.get(KEY_PIAX_IP_ADDRESS);
        int pport = (int) (config.get(KEY_PIAX_PORT));
        //String pid = (String) (config.get(KEY_PIAX_PEER_ID));
        String sip = (String) config.get(KEY_PIAX_SEED_IP_ADDRESS);
        int sport = (int) (config.get(KEY_PIAX_SEED_PORT));
        if (isEmpty(pip)) {
            msg = "Wrong peer ip address.";
            bad_param = true;
        }
        if (pport == 0) {
            msg = "Wrong peer port.";
            bad_param = true;
        }
        //if (isEmpty(pid)) {
        //    msg = "Wrong peer ID";
        //    bad_param = true;
        //}
        checkDomain();
        String pcid = (String) (config.get(KEY_PIAX_DOMAIN_NAME));
        if (sport == 0) {
            msg = "Wrong seed port.";
            bad_param = true;
        }
        if (bad_param) {
            logger.error(msg);
            return buildResponse(response.NG, msg, null);
        }

        try {
            loc = new UdpLocator(new InetSocketAddress(pip, pport));
            peer = Peer.getInstance(PeerId.newId());
            startLog += "peer instance created." + newline;
            cid = new ClusterId(pcid);
        } catch (RuntimeException e1) {
            fin();
            msg = "";
            if (loc == null) {
                msg = startLog + "Failed to create locator.";
            } else if (peer == null) {
                msg = startLog + "Failed to create peer.";
            } else if (cid == null) {
                msg = startLog + "Failed to get ClusterId.";
            }
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }

        try {
            c = peer.newBaseChannelTransport((UdpLocator) loc);
            szk = new Suzaku<Destination, LATKey>(c);
        } catch (IdConflictException | IOException e1) {
            fin();
            msg = startLog + "Failed to start peer.";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }
        startLog += "Suzaku/";

        try {
            e = new PeerMqEngineMoquette(szk, config.toMQTTProps());
        } catch (IOException | MqException e1) {
            fin();
            msg = startLog + "Failed to start moquette.";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }
        startLog += "MqEngine";
        e.setSeed(sip, sport);
        e.setClusterId(cid.toString());
        e.setCallback(new MqCallback() {
            @Override
            public void deliveryComplete(MqDeliveryToken arg0) {
                logger.debug("Launcher deliveryComplete: topic="
                        + arg0.getTopics());
            }

            @Override
            public void messageArrived(MqTopic t, MqMessage m) {
                byte[] body = m.getPayload();
                String msg = null;
                try {
                    msg = new String(body, "UTF-8");
                } catch (UnsupportedEncodingException e1) {
                    String msg2 = "Exception caused by debugging codes.";
                    String detail = stackTraceStr(e1);
                    logger.debug(msg2 + newline + detail);
                }
                logger.debug("Launcher messageArrived: topic=" + m.getTopic()
                        + " msg=" + msg);
                e.write(m);
            }
        });
        try {
            e.connect();
        } catch (MqException e1) {
            e = null;
            fin();
            msg = startLog + "peer failed to join().";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }
        startLog += "(pid=" + peer.getPeerId() + "," + " cid=" + (cid.isZeroLength()? "default" : cid)
                + ") started.";
        startDate = new Date().getTime();
        logger.info(startLog);
        return buildResponse(response.OK, startLog, String.valueOf(startDate));
    }

    public Response stop() {
        if (startDate != 0) {
            startDate = 0;
        } else {
            String msg = "MQTT already stopped.";
            logger.info(msg);
            return buildResponse(response.OK, msg, null);
        }

        try {
            if (e != null) {
                e.disconnect();
                e = null;
                fin();
            } else {
                return buildResponse(response.OK, "MQTT already stopped.", "");
            }
        } catch (MqException e1) {
            String msg = "peer failed to disconnect() or fin().";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }
        logger.info("MQTT stopped.");
        return buildResponse(response.OK, "MQTT stopped.", "");
    }

    public Response saveConfig(LinkedHashMap<String, String> data) {
        config.setConfig(data);
        MqttOnPiaxApp.configuration = config.toProperties();
        logger.debug("MqttOnPiaxApp.configuration:"
                + MqttOnPiaxApp.configuration);

        FileOutputStream os;
        try {
            os = new FileOutputStream(configFile());
            MqttOnPiaxApp.configuration.store(os, "update props");
            os.close();
        } catch (IOException e1) {
            String msg = "Failed to store config in " + configFile() + ".";
            String detail = stackTraceStr(e1);
            logger.error(msg + newline + detail);
            return buildResponse(response.NG, msg, detail);
        }
        logger.info("Stored config in " + configFile() + ".");
        return buildResponse(response.OK, "saveConfig succeeded.", "");
    }

    public String getStatistics() {
        if (e == null)
            return null;
        else
            return e.getStatistics();
    }

    public long getStartDate() {
        return startDate;
    }
    
    public void pubsubManager(){
        
    }

    private void jetty_stop() {
        if (server != null) {
            try {
                server.stop();
                server = null;
            } catch (Exception e) {
                String detail = stackTraceStr(e);
                logger.error("Failed to stop Jetty.\n" + detail);
            }
        }
    }
    
    public PeerMqEngineMoquette getEngine(){
        return e;
    }

    public static void main(String[] args) throws AgentException {
        Launcher launcher = Launcher.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                launcher.jetty_stop();
                cont = false;
            }
        });
        launcher.init(args);
        while (cont) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                String msg = "sleep error at main. exit.";
                String detail = stackTraceStr(e1);
                logger.error(msg + newline + detail);
                System.exit(1);
            }
        }
        System.exit(0);
    }

}
