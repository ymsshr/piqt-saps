package jp.piax.ofm.pubsub.web.websocket;

import java.io.IOException;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketChannel implements WebSocketListener {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(WebSocketChannel.class);

    protected WebSocketListener listener = null;
    private Session session = null;

    @Override
    public void onWebSocketConnect(Session session) {
        logger.trace("onWebSocketConnect {}", session.getRemoteAddress());
        this.session = session;
        if (listener != null)
            listener.onWebSocketConnect(session);
    }

    @Override
    public void onWebSocketClose(int close_code, String msg) {
        logger.trace("onWebSocketClose {}", session.getRemoteAddress());
        if (listener != null)
            listener.onWebSocketClose(close_code, msg);
    }

    @Override
    public void onWebSocketError(Throwable error) {
        logger.trace("onWebSocketError {} {}", session.getRemoteAddress(), error);
        if (listener != null)
            listener.onWebSocketError(error);
    }

    @Override
    public void onWebSocketText(String msg) {
        logger.trace("onWebSocketText {} {}", session.getRemoteAddress(), msg);
        if (listener != null)
            listener.onWebSocketText(msg);
    }

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        logger.trace("onWebSocketBinary {} {}", session.getRemoteAddress());
        if (listener != null)
            listener.onWebSocketBinary(payload, offset, len);
    }

    public boolean isOpen() {
        return session.isOpen();
    }

    /**
     * Send text message to connected browsers
     * 
     * @param msg
     * @throws IOException 
     */
    public void sendMessage(String msg) throws IOException {
        if (logger.isTraceEnabled()) {
            logger.trace("sendMessage {} to {} ", msg, session.getRemote().toString());
        }
        session.getRemote().sendString(msg);
    }

    public void setListener(WebSocketListener listener) {
        this.listener = listener;
    }
}
