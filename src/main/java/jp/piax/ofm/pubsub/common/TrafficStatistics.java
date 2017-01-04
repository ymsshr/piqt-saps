package jp.piax.ofm.pubsub.common;

import java.io.Serializable;

/**
 * 受信時統計情報
 */
public class TrafficStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private long message;   // 受信 publish 数
    private double delayAvg;    // 受信遅延時間の平均 (ms)
    private double delayDiv;    // 受信遅延時間の分散
    private long messageOverlay;    // Overlay 経由での受信 publish 数
    private double delayOverlayAvg; // Overlay 経由での受信の遅延時間の平均 (ms)
    private double delayOverlayDiv; // Overlay 経由での受信の遅延時間の分散
    private long messageOFM;    // OFM 経由での受信 publish 数
    private double delayOFMAvg; // OFM 経由での受信の遅延時間の平均 (ms)
    private double delayOFMDiv; // OFM 経由での受信の遅延時間の分散

    /**
     * コンストラクタ
     * @param message 受信 publish 数
     * @param delayAvg 受信遅延時間の平均 (ms)
     * @param delayDiv 受信遅延時間の分散
     * @param messageOverlay Overlay 経由での受信 publish 数
     * @param delayOverlayAvg Overlay 経由での受信の遅延時間の平均 (ms)
     * @param delayOverlayDiv Overlay 経由での受信の遅延時間の分散
     * @param messageOFM OFM 経由での受信 publish 数
     * @param delayOFMAvg OFM 経由での受信の遅延時間の平均 (ms)
     * @param delayOFMDiv OFM 経由での受信の遅延時間の分散
     */
    public TrafficStatistics(long message, double delayAvg, double delayDiv, 
            long messageOverlay, double delayOverlayAvg, double delayOverlayDiv, 
            long messageOFM, double delayOFMAvg, double delayOFMDiv) {
        if (message < 0)
            throw new IllegalArgumentException("message should be positive");
        if (delayAvg < 0)
            throw new IllegalArgumentException("delayAvg should be positive");
        if (delayDiv < 0)
            throw new IllegalArgumentException("delayDiv should be positive");
        if (messageOverlay < 0)
            throw new IllegalArgumentException("messageOverlay should be positive");
        if (delayOverlayAvg < 0)
            throw new IllegalArgumentException("delayOverlayAvg should be positive");
        if (delayOverlayDiv < 0)
            throw new IllegalArgumentException("delayOverlayDiv should be positive");
        if (messageOFM < 0)
            throw new IllegalArgumentException("messageOFM should be positive");
        if (delayOFMAvg < 0)
            throw new IllegalArgumentException("delayOFMAvg should be positive");
        if (delayOFMDiv < 0)
            throw new IllegalArgumentException("delayOFMDiv should be positive");

        this.message = message;
        this.delayAvg = delayAvg;
        this.delayDiv = delayDiv;
        this.messageOverlay = messageOverlay;
        this.delayOverlayAvg = delayOverlayAvg;
        this.delayOverlayDiv = delayOverlayDiv;
        this.messageOFM = messageOFM;
        this.delayOFMAvg = delayOFMAvg;
        this.delayOFMDiv = delayOFMDiv;
    }

    /**
     * 受信 publish 数を取得する
     * @return
     */
    public long getMessage() {
        return message;
    }

    /**
     * 受信遅延時間の平均を取得する (ms)
     * @return
     */
    public double getDelayAvg() {
        return delayAvg;
    }

    /**
     * 受信遅延時間の分散を取得する
     * @return
     */
    public double getDelayDiv() {
        return delayDiv;
    }

    /**
     * 経由での受信 publish 数を取得する
     * @return
     */
    public long getMessageOverlay() {
        return messageOverlay;
    }

    /**
     * Overlay 経由での受信の遅延時間の平均を取得する (ms)
     * @return
     */
    public double getDelayOverlayAvg() {
        return delayOverlayAvg;
    }

    /**
     * 経由での受信の遅延時間の分散を取得する
     * @return
     */
    public double getDelayOverlayDiv() {
        return delayOverlayDiv;
    }

    /**
     * OFM 経由での受信 publish 数を取得する
     * @return
     */
    public long getMessageOFM() {
        return messageOFM;
    }

    /**
     * OFM 経由での受信の遅延時間の平均を取得する (ms)
     * @return
     */
    public double getDelayOFMAvg() {
        return delayOFMAvg;
    }

    /**
     * OFM 経由での受信の遅延時間の分散を取得する
     * @return
     */
    public double getDelayOFMDiv() {
        return delayOFMDiv;
    }

    @Override
    public String toString() {
        return "TrafficStatistics [message=" + message + ", delayAvg="
                + delayAvg + ", delayDiv=" + delayDiv + ", messageOverlay="
                + messageOverlay + ", delayOverlayAvg=" + delayOverlayAvg
                + ", delayOverlayDiv=" + delayOverlayDiv + ", messageOFM="
                + messageOFM + ", delayOFMAvg=" + delayOFMAvg
                + ", delayOFMDiv=" + delayOFMDiv + "]";
    }
}