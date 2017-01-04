package jp.piax.ofm.pubsub.web;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.adaptor.WebUserAdaptorManager;
import jp.piax.ofm.pubsub.common.CommonValues;


public class SessionListener implements HttpSessionListener {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(SessionListener.class);

    protected WebUserAdaptorManager wuaManager = null;

    public SessionListener(WebUserAdaptorManager manager) {
        if (manager == null)
            throw new NullPointerException("manager should not be null");

        this.wuaManager = manager;
    }

    @Override
    public void sessionCreated(HttpSessionEvent event) {
        logger.debug("sessionCreated {} ", event.getSession().getId());
    }

    @Override
    public void sessionDestroyed(HttpSessionEvent event) {
        HttpSession session = event.getSession();
        String userid = (String) session.getAttribute(CommonValues.SESSION_USERID);
        logger.debug("sessionDestroyed {} {}", event.getSession().getId(), userid);
        if (userid != null) {
            wuaManager.leaveUser(userid);
        }
    }
}
