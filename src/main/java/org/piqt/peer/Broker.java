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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.eclipse.moquette.server.Server;
import org.eclipse.moquette.server.config.ClasspathConfig;
import org.eclipse.moquette.server.config.IConfig;
import org.piax.agent.AgentException;
import org.piax.agent.AgentId;
import org.piqt.MqException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.PubSubManagerImpl;
import jp.piax.ofm.pubsub.PubSubManagerImplConfig;
import jp.piax.ofm.pubsub.piax.PubSubAgentHomeImpl;

public class Broker {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class
            .getPackage().getName());

    static int port = 1883;

    static Map<Thread, PeerMqEngineMoquette> engineMap = new HashMap<Thread, PeerMqEngineMoquette>();

    private IConfig config = null;
    private Properties properties = null;
    private Server mqttBroker = null;
    PeerMqEngineMoquette engine;
    
    private PubSubManagerImpl pubSubManager;
    private String agentName;
    private PubSubAgentHomeImpl home = null;

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
        home = pubSubManager.getPubSubAgentHome();

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
