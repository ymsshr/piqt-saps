package jp.piax.ofm.pubsub.common;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.kmkt.util.SimpleFuture;


/**
 * publish メッセージ
 */
public class PublishMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String userId;
    private final String topic;
    private final String content;
    private final long publishId;   // publish 操作の ID (重複メッセージ除外用)
    private long publishTime;   // メッセージ送信時刻
    private final PublishPath publishPath;  // Publish 配信経路
    private final InetSocketAddress ofmAddress;   // OFM アドレス
    private final String discoveryCallKey;  // discoveryCall時の Key
    private transient final SimpleFuture<?> loopbackFuture;   // loopbak 待ち処理のための future

    private List<TransPathInfo> sgPath = new ArrayList<>(); // 転送経路のリスト

    /**
     * 配信経路を表す
     */
    public enum PublishPath {
        /** Overlay 経由 */
        Overlay,
        /** OFM 経由 */
        OFM
    }

    /**
     * Openflow multicast 配信用コンストラクタ
     * @param userid
     * @param topic
     * @param content
     * @param ofmaddress OFM アドレス
     * @param path 
     * @param dckey discoveryCall の探索キー
     */
    public PublishMessage(String userid, String topic, 
                String content, long publishid, InetSocketAddress ofmaddress, PublishPath path, String dckey) {
        if (userid == null)
            throw new NullPointerException("userid should not be null");
        if (userid.isEmpty())
            throw new IllegalArgumentException("userId should not be empty");
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (content == null)
            throw new NullPointerException("content should not be null");
        if (content.isEmpty())
            throw new IllegalArgumentException("content should not be empty");
        if (path == null)
            throw new NullPointerException("path should not be null");
        if (path == PublishPath.OFM && ofmaddress == null)
            throw new IllegalArgumentException("ofmaddress should not be null when path is OFM");
        if (dckey == null)
            throw new NullPointerException("dckey should not be null");
        if (dckey.isEmpty())
            throw new IllegalArgumentException("dckey should not be empty");

        this.userId = userid;
        this.topic = topic;
        this.content = content;
        this.publishId = publishid;
        this.ofmAddress = ofmaddress;
        this.publishPath = path;
        if (path == PublishPath.OFM) {
            loopbackFuture = new SimpleFuture<>();
        } else {
            loopbackFuture = null;
        }
        this.discoveryCallKey = dckey;
    }

    /**
     * UserID を取得する
     * @return
     */
    public String getUserId() {
        return userId;
    }

    /**
     * topic を取得する
     * @return
     */
    public String getTopic() {
        return topic;
    }

    /**
     * content を取得する
     * @return
     */
    public String getContent() {
        return content;
    }

    /**
     * Publish ID を取得する
     * @return
     */
    public long getPublishId() {
        return publishId;
    }

    /**
     * 送信時刻を設定する
     * @param time System.currentTimeMillis で取得できるシステム時刻
     */
    public void setPublishTime(long time) {
        if (time < 0)
            throw new IllegalArgumentException("time should be a positive number");

        publishTime = time;
    }

    /**
     * 送信時刻を取得する
     * @return
     */
    public long getPublishTime() {
        return publishTime;
    }

    /**
     * 配信経路を取得する
     * @return
     */
    public PublishPath getPublishPath() {
        return publishPath;
    }

    /**
     * OFM アドレスを取得する
     * @return OFM アドレス
     */
    public InetSocketAddress getOfmAddress() {
        return ofmAddress;
    }

    /**
     * discvoeryCall 時の探索キーを取得する
     * @return
     */
    public String getDiscoveryCallKey() {
        return discoveryCallKey;
    }

    /**
     * 転送経路の情報をリスト末に追加する
     * @param path 転送経路の情報
     */
    public void addTransPath(TransPathInfo path) {
        if (path == null)
            throw new IllegalArgumentException("path should not be null");

        synchronized (sgPath) {
            sgPath.add(path);
        }
    }

    /**
     * 転送経路のリストを取得する
     * @return 転送経路のリストの unmodifiableList
     */
    public List<TransPathInfo> getTransPath() {
        return Collections.unmodifiableList(sgPath);
    }

    /**
     * OFM loopback を受信した場合に呼び出す
     */
    public void receiveLoopbackMsg() {
        if (loopbackFuture != null) {
            loopbackFuture.set(null);
        }
    }

    /**
     * OFM loopback の受信を待機する
     * @param timeout 受信タイムアウト (ms)
     * @return 受信できた場合は true タイムアウト、又は Interrupt された場合は false
     */
    public boolean waitLoopbackMsg(long timeout) {
        try {
            return loopbackFuture.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
}
