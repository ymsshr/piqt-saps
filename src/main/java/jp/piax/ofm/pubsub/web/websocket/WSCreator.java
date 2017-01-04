package jp.piax.ofm.pubsub.web.websocket;

import javax.servlet.http.HttpSession;

import jp.piax.ofm.pubsub.adaptor.WebUserAdaptor;
import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;
import jp.piax.ofm.pubsub.common.CommonValues;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocketCreator
 * 
 * WebSocket として WebSocketChannel を生成し、
 * 認証された UserID に対応する WebUserAdaptor と連携させる。
 */
public class WSCreator implements WebSocketCreator {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(WSCreator.class);

    protected WebUserAdaptorManager wuaManager = null;

    /**
     * 
     * @param manager
     */
    public WSCreator(WebUserAdaptorManager manager) {
        if (manager == null)
            throw new NullPointerException("manager should not be null");

        this.wuaManager = manager;
    }

    @Override
    public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        logger.debug("createWebSocket");

        HttpSession session = (HttpSession) req.getSession();
        if (session == null || session.isNew()) {
            logger.error("No valid session");
            return null;
        }

        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        if (userid == null) {
            session.invalidate();
            logger.error("Not authorized session");
            return null;
        }

        logger.debug("UserID {} ", userid);

        WebSocketChannel socket = new WebSocketChannel();

        // WebSocket を UserId に対応する WebUserAdaptor に登録する
        WebUserAdaptor wua = wuaManager.getWebUserAdaptor(userid);
        wua.registerWebSocket(socket);

        return socket;
    }
}
