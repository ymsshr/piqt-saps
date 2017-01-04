package jp.piax.ofm.pubsub;

import java.util.List;

import jp.piax.ofm.pubsub.common.PubSubException;

/**
 * 各ユーザに対応する Pub/Sub インタフェース
 * 
 * PubSubManager#getUserPubSub により取得し、Publish/Subscribe 操作は
 * このインタフェースを介して行う。
 */
public interface UserPubSub {
    /**
     * Publish されたメッセージを受信した際の callback interface
     */
    public interface MessageListener {
        /**
         * Publish されたメッセージを受信した際に callback されるメソッド
         * @param sender メッセージ送信元 User ID
         * @param topic トピック名
         * @param content コンテンツ
         */
        public void onMessage(String sender, String topic, String content);
    }

    /**
     * ユーザIDを取得する
     * @return ユーザID
     */
    public String getUserId();

    /**
     * メッセージを publish する
     * @param topic publish するトピック名
     * @param content コンテンツ
     * @throws PubSubException
     */
    public void publish(String topic, String content) throws PubSubException;

    /**
     * topic を subscribe する
     * @param topic
     * @throws PubSubException
     */
    public void subscribe(String topic) throws PubSubException;

    /**
     * topic を unsubscribe する
     * @param topic
     * @throws PubSubException
     */
    public void unsubscribe(String topic) throws PubSubException;

    /**
     * Publish されたメッセージを受信した際の callback を設定する
     * @param listener
     */
    public void setMessageListener(MessageListener listener);

    /**
     * subscribe している topic のリストを取得する
     * @return subscribe している topic のリスト
     */
    public List<String> getSubscribedTopic();
}
