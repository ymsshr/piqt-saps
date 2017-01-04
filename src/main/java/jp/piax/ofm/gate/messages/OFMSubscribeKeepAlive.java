package jp.piax.ofm.gate.messages;

import java.net.InetSocketAddress;

/**
 * OFGate から subscriber への keep-alive メッセージ
 */
public class OFMSubscribeKeepAlive extends TopicAddressPair {
    private static final long serialVersionUID = 1L;

    /**
     * コンストラクタ
     * 
     * @param topic 問い合わせされた topic 名
     * @param ofmaddress 対応する Openflow multicast アドレス
     */
    public OFMSubscribeKeepAlive(String topic, InetSocketAddress ofmaddress) {
        super(topic, ofmaddress);
        if (ofmaddress == null)
            throw new NullPointerException("ofmaddress should not be null");
    }

    /**
     * topic に対応する Openflow multicast アドレスを取得する
     * @return
     */
    public InetSocketAddress getOFMAddress() {
        return super.getAddress();
    }
}
