package jp.piax.ofm.pubsub.web.websocket;

import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket Servlet
 */
public class WSServlet extends WebSocketServlet {
    private static final long serialVersionUID = 1L;

    /*--- logger ---*/
    private static Logger logger = LoggerFactory.getLogger(WSServlet.class);

    protected WebUserAdaptorManager manager = null;

    /**
     * 
     * @param manager
     */
    public WSServlet(WebUserAdaptorManager manager) {
        if (manager == null)
            throw new NullPointerException("manager should not be null");

        this.manager = manager;
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        logger.debug("configure");
        factory.setCreator(new WSCreator(this.manager));
    }
}
