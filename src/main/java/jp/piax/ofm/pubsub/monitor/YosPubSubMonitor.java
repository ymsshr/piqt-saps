package jp.piax.ofm.pubsub.monitor;


public interface YosPubSubMonitor extends PubSubMonitor {
    /**
     * topic を ALM にて subscriebe している subscriber を OFM メンバーに加えるか否かを判定する
     * true が返された場合、 getSubscriberAddress は OFM メンバーに加える subscriber の OFM 受信アドレスを返す
     * Openflow multicast 網が構築されていない場合は false を返す
     * @param topic
     * @return true subscriber を OFM メンバーに加える false 加えない
     */
    boolean addSubscriberToOFM(String topic);
}
