package jp.piax.ofm.gate.common;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import jp.piax.ofm.gate.messages.IPMACPair;

/**
 * setupOFM の処理結果の返却値
 * 
 * OFM アドレスは存在するが要求された subscriber と現在の OFM メンバが
 * 合わない場合があるため、このクラスを用いて処理結果を返す
 */
public class SetupOFMResult implements Serializable {
    private static final long serialVersionUID = 1L;

    /** setupOFM に渡された topic */
    private final String topic;
    /** setupOFM の処理が完全に行われた場合は true */
    private final boolean fullySucceed;
    /** topic に対応する OFM アドレス 存在しない場合は null */
    private final InetSocketAddress ofmAddress;
    /** setupOFM 時に既に登録されていた subscriber の OFM receiver address の Set */
    private final Set<IPMACPair> ofmMembers;


    /**
     * コンストラクタ
     * @param topic setupOFM に渡された topic
     * @param fully_succeed setupOFM の処理が完全に行われた場合は true 失敗、あるいは既設かつ subscriber が一致しない場合は false
     * @param ofmaddress topic に対応する OFM アドレス 存在しない場合は null
     * @param ofmmmbers setupOFM 時に既に登録されていた subscriber の OFM receiver address の Set
     */
    public SetupOFMResult(String topic, boolean fully_succeed,
            InetSocketAddress ofmaddress, Set<IPMACPair> ofmmmbers) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be null");
        this.topic = topic;
        this.fullySucceed = fully_succeed;
        this.ofmAddress = ofmaddress;
        if (ofmmmbers == null) {
            this.ofmMembers = Collections.emptySet();
        } else {
            this.ofmMembers = new HashSet<IPMACPair>(ofmmmbers);
        }
    }

    /**
     * setupOFM に渡された topic を取得する
     * @return
     */
    public String getTopic() {
        return topic;
    }

    /**
     * setupOFM の処理が完全に行われた場合は true それ以外は false を返す
     * 
     * @return
     */
    public boolean isFullySucceed() {
        return fullySucceed;
    }

    /**
     * topic に対応する OFM アドレスを取得する 存在しない場合は null
     * @return
     */
    public InetSocketAddress getOfmAddress() {
        return ofmAddress;
    }

    /**
     * setupOFM 呼び出し時に既に OFM メンバとなっていた ssubscriber の OFM receiver address の unmodifiableSet を取得する
     * @return
     */
    public Set<IPMACPair> getOfmMembers() {
        return Collections.unmodifiableSet(ofmMembers);
    }
}
