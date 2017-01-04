package jp.piax.ofm.gate.piax.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import jp.piax.ofm.gate.OFMManager;
import jp.piax.ofm.gate.common.CommonValues;
import jp.piax.ofm.gate.common.NullAddress;
import jp.piax.ofm.gate.common.SetupOFMResult;
import jp.piax.ofm.gate.messages.IPMACPair;

import org.json.JSONException;
import org.piax.agent.Agent;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.RemoteCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OFGate agent
 * PubSubAgent から OFM の操作リクエストに対応する
 */
public class OFGate extends Agent implements OFGateIf {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(OFGate.class);

    protected OFMManager topicManager;

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#setOFGateManager(jp.piax.ofm.gate.OFGateManager)
     */
    @Override
    public void setOFGateManager(OFMManager manager) {
        if (manager == null)
            throw new NullPointerException("manager should not be null");

        topicManager = manager;
    }

    @Override
    public void onCreation() {
        try {
            this.setAttrib(CommonValues.OFGATE_ATTRIB, CommonValues.OFGATE_VALUE, true);
        } catch (IllegalArgumentException | IncompatibleTypeException  e) {
            logger.error("Cannot register the OFGate agent to overlay", e);
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#setupOFM(java.lang.String, java.util.List)
     */
    @Override
    @RemoteCallable
    public SetupOFMResult setupOFM(String topic, Set<IPMACPair> subscribers) throws IOException, JSONException {
        logger.info("setupOFM for topic:[{}]. subscribers:[{}]", topic, subscribers.size());
        if (logger.isDebugEnabled()) {
            logger.debug("Requested subscribers are... {}", IPMACPair.convertToOFMAddrString(subscribers));
        }
        try {
            SetupOFMResult result = topicManager.setupOFM(topic, subscribers);
            logger.info("setupOFM for topic:[{}] returns {}", topic, result.getOfmAddress());
            return result;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#getOFMAddress(java.lang.String)
     */
    @Override
    @RemoteCallable
    public InetSocketAddress getOFMAddress(String topic) {
        logger.info("getOFMAddress for topic:[{}]", topic);
        try {
            InetSocketAddress result = topicManager.getOFMAddress(topic);
            logger.info("getOFMAddress for topic:[{}] returns {}", topic, result);
            // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
            // null を返す代わりに NULl object を返す
            if (result != null) {
                return result;
            } else {
                return NullAddress.NULL_ADDR;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#addSubscriber(java.lang.String, jp.piax.ofm.gate.messages.IPMACPair)
     */
    @Override
    @RemoteCallable
    public InetSocketAddress addSubscriber(String topic, IPMACPair receiveaddress) throws IOException, JSONException {
        logger.info("addSubscriber for topic:[{}]. subscribers:[1]", topic);
        if (logger.isDebugEnabled()) {
            logger.debug("Adding subscriber is... {}", IPMACPair.convertToOFMAddrFormat(receiveaddress));
        }
        try {
            InetSocketAddress result = topicManager.subscribeRequest(topic, receiveaddress);
            logger.info("addSubscriber for topic:[{}] returns {}", topic, result);
            // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
            // null を返す代わりに NULl object を返す
            if (result != null) {
                return result;
            } else {
                return NullAddress.NULL_ADDR;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#addSubscribers(java.lang.String, java.util.Set)
     */
    @Override
    @RemoteCallable
    public InetSocketAddress addSubscribers(String topic, Set<IPMACPair> receiveaddress) throws IOException, JSONException {
        logger.info("addSubscribers for topic:[{}]. subscribes:[{}]", topic, receiveaddress.size());
        if (logger.isDebugEnabled()) {
            logger.debug("Adding subscribers are... {}", IPMACPair.convertToOFMAddrString(receiveaddress));
        }
        try {
            InetSocketAddress result = topicManager.subscribeRequest(topic, receiveaddress);
            logger.info("addSubscribers for topic:[{}] returns {}", topic, result);
            // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
            // null を返す代わりに NULl object を返す
            if (result != null) {
                return result;
            } else {
                return NullAddress.NULL_ADDR;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#removeSubscriber(java.lang.String, jp.piax.ofm.gate.messages.IPMACPair)
     */
    @Override
    @RemoteCallable
    public InetSocketAddress removeSubscriber(String topic, IPMACPair receiveaddress) throws IOException, JSONException {
        logger.info("removeSubscriber for topic:[{}]. subscribers:[1]", topic);
        if (logger.isDebugEnabled()) {
            logger.debug("Removing subscriber is... {}", IPMACPair.convertToOFMAddrFormat(receiveaddress));
        }
        try {
            InetSocketAddress result = topicManager.unsubscribeRequest(topic, receiveaddress);
            logger.info("removeSubscriber for topic:[{}] returns {}", topic, result);
            // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
            // null を返す代わりに NULl object を返す
            if (result != null) {
                return result;
            } else {
                return NullAddress.NULL_ADDR;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    /* (非 Javadoc)
     * @see jp.piax.ofm.gate.piax.agent.OFGateIf#removeSubscribers(java.lang.String, java.util.Set)
     */
    @Override
    @RemoteCallable
    public InetSocketAddress removeSubscribers(String topic, Set<IPMACPair> receiveaddress) throws IOException, JSONException {
        logger.info("removeSubscribers for topic:[{}]. subscribers:[{}]", topic, receiveaddress.size());
        if (logger.isDebugEnabled()) {
            logger.debug("Removing subscribers are... {}", IPMACPair.convertToOFMAddrString(receiveaddress));
        }
        try {
            InetSocketAddress result = topicManager.unsubscribeRequest(topic, receiveaddress);
            logger.info("removeSubscribers for topic:[{}] returns {}", topic, result);
            // XXX PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
            // null を返す代わりに NULl object を返す
            if (result != null) {
                return result;
            } else {
                return NullAddress.NULL_ADDR;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }
}
