package jp.piax.ofm.pubsub.adaptor;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.UserPubSub;
import jp.piax.ofm.pubsub.common.PubSubException;
import jp.piax.ofm.pubsub.web.websocket.WebSocketChannel;

/**
 * Web からのアクセスと PubSub 系を繋ぐアダプタクラス
 * 
 * ユーザ(Web session)毎に生成され、ユーザに対応する UserPubSubInterface との橋渡しをする。
 */
public class WebUserAdaptor implements WebSocketListener, UserPubSub.MessageListener {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(WebUserAdaptor.class);

    private Set<WebSocketChannel> sockets = new HashSet<WebSocketChannel>();
    private UserPubSub pubSub = null;

    public WebUserAdaptor(UserPubSub pubsub) {
        if (pubsub == null)
            throw new NullPointerException("pubsub should not be null");

        pubSub = pubsub;

        pubSub.setMessageListener(this);
    }

    /**
     * 
     * @param socket
     */
    public void registerWebSocket(WebSocketChannel socket) {
        if (socket == null)
            throw new NullPointerException("socket should not be null");

        synchronized (sockets) {
            sockets.add(socket);
        }
        socket.setListener(this);
    }

    public void publish(String topic, String content) throws PubSubException {
        pubSub.publish(topic, content);
    }

    public void subscribe(String topic) throws PubSubException {
        pubSub.subscribe(topic);
    }

    public void unsubscribe(String topic) throws PubSubException {
        pubSub.unsubscribe(topic);
    }

    public List<String> getSubscribeTopic() {
        return pubSub.getSubscribedTopic();
    }

    public String getUserId() {
        return pubSub.getUserId();
    }

    //--- WebSocketListener callback
    // WebSocket --> this
    @Override
    public void onWebSocketConnect(Session session) {
    }

    @Override
    public void onWebSocketClose(int close_code, String msg) {
    }

    @Override
    public void onWebSocketError(Throwable error) {
    }

    @Override
    public void onWebSocketText(String msg) {
        logger.trace(msg);
    }

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        
    }


    //--- MessageListener callback
    // PubSub system --> this
    @Override
    public void onMessage(String sender, String topic, String content) {
        logger.trace("onMessage {} {} {}", sender, topic, content);
        try {
            JSONObject json = new JSONObject();
            json.put("sender", sender);
            json.put("topic", topic);
            json.put("content", content);
            String msg = json.toString();

            synchronized (sockets) {
                Set<WebSocketChannel> remove_soc = new HashSet<WebSocketChannel>();
                for (WebSocketChannel socket : sockets) {
                    try {
                        socket.sendMessage(msg);
                    } catch (IOException e) {
                        String errmsg = e.getMessage();
                        logger.debug("IOException at sending to WebSocket : {}", errmsg);
                        logger.trace(errmsg, e);
                        remove_soc.add(socket);
                    } catch (WebSocketException e) {
                        String errmsg = e.getMessage();
                        logger.debug("IOException at sending to WebSocket : {}", errmsg);
                        logger.trace(errmsg, e);
                        remove_soc.add(socket);
                    }
                }
                if (!remove_soc.isEmpty()) {
                    for (WebSocketChannel socket : sockets) {
                        socket.setListener(null);
                    }
                    sockets.removeAll(remove_soc);
                    logger.debug("Remove errored WebSocket : {}", remove_soc.size());
                }
            }
        } catch (JSONException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
