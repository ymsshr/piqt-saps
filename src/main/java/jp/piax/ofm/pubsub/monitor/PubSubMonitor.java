package jp.piax.ofm.pubsub.monitor;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Set;

import jp.piax.ofm.gate.messages.IPMACPair;
import jp.piax.ofm.pubsub.common.SubscriberInfo;
import jp.piax.ofm.pubsub.misc.OFMAddressCache;

public interface PubSubMonitor extends Serializable {
    /**
     * モニタしているトピックを返す
     * @return モニタしているトピックの Set トピックが無い場合は空の Set を返す
     */
    Set<String> getMonitoringTopics();

    /**
     * OFM 操作の判断に用いる publish された topic を与える
     * @param topic
     */
    void putPublish(String topic);

    /**
     * OFM 操作の判断に用いる SubscriberInfo を与える
     * @param info
     */
    void putSubscriberInfo(String topic, Set<SubscriberInfo> info);

    /**
     * publish 時に Openflow multicast を使うか否かを判定する
     * このメソッドは publish ごとに呼び出され、topic への publish 
     * に Openflow multicast を使うか否かを返す
     * @param topic
     * @return true Openflow multicast を使う false Openflow multicast を使わない
     */
    boolean useOFM(String topic);

    /**
     * topic に対する Openflow multicast 網を作成するか否かを判定する
     * Openflow multicast 網が構築済みの場合は false を返さなければならない
     * @param topic
     * @return true Openflow multicast 網を作成する false Openflow multicast 網を作成しない
     */
    boolean setupOFM(String topic);

    /**
     * Openflow multicast 網の作成に用いる Subscriber のアドレスを取得する
     * @param topic
     * @return 各Subscriber の OFM 受信アドレスの Set
     */
    Set<IPMACPair> getSubscriberAddress(String topic);

    /**
     * OFMAddressCache を Monitor に与える
     * @param cache OFMAddressCacheのインスタンス
     */
    void attachOFMAddressCache(OFMAddressCache cache);

    /**
     * Openflow multicast 網の OFM アドレスを設定する
     * 同時に内部に持つ OFMAddressCache を更新する
     * ofmaddress に null を与えた場合は OFM アドレスが無効化されたことを意味する
     * @param topic
     * @param ofmaddress topic に対応する OFM アドレス
     */
    void setOFMAddress(String topic, InetSocketAddress ofmaddress);

    /**
     * 指定された topic の OFM アドレスを取得する
     * キャッシュされていない場合、タイムアウトしている場合は null を返す
     * @param topic
     * @return OFM アドレス キャッシュされていない場合、タイムアウトしている場合は null
     */
    InetSocketAddress getOFMAddress(String topic);
}
