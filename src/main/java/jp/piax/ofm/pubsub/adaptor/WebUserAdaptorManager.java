package jp.piax.ofm.pubsub.adaptor;

import java.util.HashMap;
import java.util.Map;

import org.piax.agent.AgentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.PubSubManager;
import jp.piax.ofm.pubsub.UserPubSub;

/**
 * WebUserAdaptor を振り出す管理クラス
 */
public class WebUserAdaptorManager {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(WebUserAdaptorManager.class);

    protected PubSubManager manager = null;
    protected Map<String, WebUserAdaptor> webUserAdaptors = new HashMap<String, WebUserAdaptor>();

    /**
     * 
     * @param man
     */
    public WebUserAdaptorManager(PubSubManager man) {
        if (man == null)
            throw new NullPointerException("man should not be null");

        this.manager = man;
    }

    /**
     * 
     * @param userid
     * @return
     */
    public WebUserAdaptor getWebUserAdaptor(String userid) {
        logger.debug("getWebUserAdaptor {}", userid);

        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");

        synchronized(webUserAdaptors) {
            if (!webUserAdaptors.containsKey(userid)) {
                UserPubSub pubsub;
                try {
                    pubsub = manager.getUserPubSub(userid);
                    if (pubsub == null)
                        return null;
                } catch (AgentException e) {
                    logger.error("", e);
                    return null;
                }

                WebUserAdaptor wua = new WebUserAdaptor(pubsub);
                webUserAdaptors.put(userid, wua);
            }
            return webUserAdaptors.get(userid);
        }
    }

    /**
     * 
     * @param userid
     */
    public void leaveUser(String userid) {
        logger.debug("leaveUser {}", userid);

        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userid should not be empty");

        synchronized(webUserAdaptors) {
            if (webUserAdaptors.containsKey(userid)) {
                webUserAdaptors.remove(userid);
            }
        }
    }
}
