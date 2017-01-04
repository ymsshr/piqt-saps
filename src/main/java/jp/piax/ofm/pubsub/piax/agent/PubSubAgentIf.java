package jp.piax.ofm.pubsub.piax.agent;

import java.util.List;

import jp.piax.ofm.pubsub.common.PublishMessage;
import jp.piax.ofm.pubsub.common.SubscriberInfo;

import org.piax.agent.AgentIf;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.ov.NoSuchOverlayException;
import org.piax.gtrans.RemoteCallable;

public interface PubSubAgentIf extends AgentIf {

    /**
     * callback listener for message receiving
     */
    public interface OnMessageListener {
        /**
         * callback when it receive a publish message
         * @param sender
         * @param topic
         * @param content
         */
        public void onReceive(String sender, String topic, String content);
    }

    /**
     * set listener for message receiving
     * @param listener
     */
    public void setCallbackListener(OnMessageListener listener);

    /**
     * set user id to this agent
     * @param userid
     */
    public void setUserId(String userid);

    /**
     * get userid
     * @return userid
     */
    public String getUserId();

    /**
     * get a subscribed topic list
     * @return subscribed topic list
     */
    public List<String> getSubscribedTopic();

    /**
     * subscribe topic
     * @param topic
     * @throws IllegalArgumentException
     * @throws IncompatibleTypeException
     * @throws NoSuchOverlayException
     */
    public void subscribe(String topic) throws IllegalArgumentException, IncompatibleTypeException, NoSuchOverlayException;

    /**
     * subscribe topic
     * @param topic
     */
    public void unsubscribe(String topic);

    /**
     * publish topic with content
     * @param topic
     * @param content
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     */
    public void publish(String topic, String content) throws IllegalArgumentException, NoSuchOverlayException, IncompatibleTypeException;

    /**
     * callback when it receive a publish message
     * @param msg
     * @return OFM address if exists, or null
     */
    @RemoteCallable
    public SubscriberInfo onReceivePublish(PublishMessage msg);


    /**
     * get subscriber statistics and OFM address
     * @param topic
     */
    @RemoteCallable
    public SubscriberInfo getSubscriberInfo(String topic);
}
