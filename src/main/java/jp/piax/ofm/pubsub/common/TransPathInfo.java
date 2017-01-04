package jp.piax.ofm.pubsub.common;

import java.io.Serializable;

import org.piax.common.PeerId;

/**
 * 経路情報として 1 パスごとに保持させる情報
 */
public class TransPathInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private PeerId pid;
    private long time;

    /**
     * 
     * @param pid PeerID
     */
    public TransPathInfo(PeerId pid) {
        if (pid == null)
            throw new IllegalArgumentException("pid should not be null");
        
        this.pid = pid;
        this.time = System.currentTimeMillis();
    }

    /**
     * PeerIDを取得する
     * @return PeerID
     */
    public PeerId getPid() {
        return pid;
    }

    /**
     * 記録timeを取得する
     * @return time
     */
    public long getTime() {
        return time;
    }

    @Override
    public String toString() {
        String pidstr = pid.toHexString();
        if (10 < pidstr.length()) {
            pidstr = pidstr.substring(0, 6) + "...";    // 長い場合は頭 7 文字に省略
        }
        return pidstr;
    }
}
