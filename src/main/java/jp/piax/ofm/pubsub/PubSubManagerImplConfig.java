package jp.piax.ofm.pubsub;

import java.io.File;

import jp.piax.ofm.pubsub.piax.agent.PubSubAgent;
import jp.piax.ofm.trans.OFMUdpLocator;

import org.piax.agent.Agent;
import org.piax.common.PeerLocator;

/**
 * PubSubManagerImpl のコンストラクタパラメータクラス
 */
public class PubSubManagerImplConfig {

    public static final Class<? extends Agent> DEFAULT_PUBSUB_AGENTCLASS = PubSubAgent.class;

    private final PeerLocator peerLocator;  // このピアの PeerLocator
    private final PeerLocator seedLocator;  // Seed peer の PeerLocator
    private final String peerName;          // ピア名
    private final File[] agClassPath;       // Agent class を置くパス
    private final Class<? extends Agent> pubsubAgent;   // PubSubAgentIf 実装クラス
    private final OFMUdpLocator ofmLocator;    // Openflow Multicast 送受信 UdpLocator
    private final long ofmCacheTimeout;     // OFM アドレスキャッシュのキャッシュ保持時間 (ms)

    /**
     * コンストラクタ
     * @param peerlocator このピアの PeerLocator
     * @param seedlocator Seed peer の PeerLocator
     * @param peername ピア名
     * @param agclasspath Agent class を置くパス null指定時はカレントディレクトリ "." となる
     * @param pubsubagent PubSubAgentIf 実装クラス
     * @param ofmlocator Openflow Multicast 送受信 UdpLocator
     * @param ofmcachetimeout OFM アドレスキャッシュのキャッシュ保持時間 (ms)
     */
    public PubSubManagerImplConfig(PeerLocator peerlocator,
            PeerLocator seedlocator,
            String peername, 
            String agclasspath,
            Class<? extends Agent> pubsubagent,
            OFMUdpLocator ofmlocator,
            long ofmcachetimeout) {
        if (peerlocator == null)
            throw new NullPointerException("peerlocator should not be null");
        if (peername == null)
            throw new NullPointerException("peername should not be null");
        if (peername.isEmpty())
            throw new IllegalArgumentException("peername should not be empty");
        if (ofmlocator == null)
            throw new NullPointerException("ofmlocator should not be null");

        // connect to myself if seedLocator == null
        if (seedlocator == null) {
            seedlocator = peerlocator;
        }

        if (agclasspath == null) {
            agclasspath = ".";
        }

        if (pubsubagent == null) {
            pubsubagent = DEFAULT_PUBSUB_AGENTCLASS;
        }

        this.peerLocator = peerlocator;
        this.seedLocator = seedlocator;
        this.peerName = peername;
        this.agClassPath = new File[]{new File(agclasspath)};
        this.pubsubAgent = pubsubagent;
        this.ofmLocator = ofmlocator;
        this.ofmCacheTimeout = ofmcachetimeout;
    }

    /**
     * Peer用Locatorを取得する
     * @return
     */
    public PeerLocator getPeerLocator() {
        return peerLocator;
    }

    /**
     * SeedピアのLocatorを取得する
     * @return
     */
    public PeerLocator getSeedLocator() {
        return seedLocator;
    }

    /**
     * ピア名を取得する
     * @return
     */
    public String getPeerName() {
        return peerName;
    }

    /**
     * Agent class を置くパスを取得する
     * @return
     */
    public File[] getAgClassPath() {
        return agClassPath;
    }

    /**
     * PubSubAgentIf 実装クラスを取得する
     */
    public Class<? extends Agent> getPubsubAgent() {
        return pubsubAgent;
    }

    /**
     * Openflow Multicast 送受信 UdpLocator を取得する
     * @return
     */
    public OFMUdpLocator getOFMLocator() {
        return ofmLocator;
    }

    /**
     * OFM アドレスキャッシュのキャッシュ保持時間を取得する (ms)
     * @return
     */
    public long getOFMCacheTimeout() {
        return ofmCacheTimeout;
    }
}
