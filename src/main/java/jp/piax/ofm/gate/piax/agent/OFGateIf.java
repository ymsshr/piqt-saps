package jp.piax.ofm.gate.piax.agent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

import jp.piax.ofm.gate.OFMManager;
import jp.piax.ofm.gate.common.SetupOFMResult;
import jp.piax.ofm.gate.messages.IPMACPair;

import org.json.JSONException;
import org.piax.agent.AgentIf;
import org.piax.gtrans.RemoteCallable;

public interface OFGateIf extends AgentIf {
    /**
     * agent に OFGateManager を設定する
     * @param manager
     */
    void setOFGateManager(OFMManager manager);

    /**
     * topicに 対応する Openflow multicast アドレスを生成する
     * @param topic
     * @param subscribers OFM 受信アドレスと対応する subscriber の OFM ポート情報
     * @return topicに 対応する Openflow multicast アドレスを含む SetupOFMResult
     * @throws JSONException 
     * @throws IOException 
     */
    @RemoteCallable
    SetupOFMResult setupOFM(String topic, Set<IPMACPair> subscribers) throws IOException, JSONException;

    /**
     * topic に対応する Openflow multicast アドレスを要求する
     * @param topic 問い合わせする topic 名
     * @return topicに 対応する Openflow multicast アドレス, Openflow multicast アドレスが無い場合は null を返す
     */
    @RemoteCallable
    InetSocketAddress getOFMAddress(String topic);


    /**
     * topic に対応する Openflow multicast グループに subscriber を追加する
     * 既に Openflow multicast グループに含まれている場合は無視される
     * @param topic
     * @param topic 追加対象の topic 
     * @param receiveaddress 追加する subscriber の Openflow multicast 受信アドレス
     * @return topicに 対応する Openflow multicast アドレス, Openflow multicast アドレスが無い場合は null を返す
     * @throws JSONException 
     * @throws IOException 
     */
    @RemoteCallable
    public InetSocketAddress addSubscriber(String topic, IPMACPair receiveaddress) throws IOException, JSONException;

    /**
     * topic に対応する Openflow multicast グループに subscriber を追加する
     * 既に Openflow multicast グループに含まれている場合は無視される
     * @param topic
     * @param topic 追加対象の topic 
     * @param receiveaddress 追加する subscriber の Openflow multicast 受信アドレス
     * @return topicに 対応する Openflow multicast アドレス, Openflow multicast アドレスが無い場合は null を返す
     * @throws JSONException 
     * @throws IOException 
     */
    @RemoteCallable
    public InetSocketAddress addSubscribers(String topic, Set<IPMACPair> receiveaddress) throws IOException, JSONException;

    /**
     * topic に対応する Openflow multicast グループから subscriber を削除する
     * Openflow multicast グループに含まれていない subscriber を与えた場合は無視される
     * この操作によって Openflow multicast グループが空になった場合は、 OFM アドレスは無効化される
     * @param topic
     * @param topic 削除対象の topic 
     * @param receiveaddress 削除する subscriber の Openflow multicast 受信アドレス
     * @return topic に対応する Openflow multicast アドレス, Openflow multicast アドレスは無効化された場合は null
     * @throws JSONException 
     * @throws IOException 
     */
    @RemoteCallable
    public InetSocketAddress removeSubscriber(String topic, IPMACPair receiveaddress) throws IOException, JSONException ;

    /**
     * topic に対応する Openflow multicast グループから subscriber を削除する
     * Openflow multicast グループに含まれていない subscriber を与えた場合は無視される
     * この操作によって Openflow multicast グループが空になった場合は、 Openflow multicast アドレスは無効化される
     * @param topic
     * @param topic 削除対象の topic 
     * @param receiveaddress 削除する subscriber の Openflow multicast 受信アドレス
     * @return topic に対応する Openflow multicast アドレス, Openflow multicast アドレスは無効化された場合は null
     * @throws JSONException 
     * @throws IOException 
     */
    @RemoteCallable
    public InetSocketAddress removeSubscribers(String topic, Set<IPMACPair> receiveaddress) throws IOException, JSONException ;
}
