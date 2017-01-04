package jp.piax.ofm.pubsub.misc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.piax.ofm.pubsub.common.TrafficStatistics;
import jp.piax.ofm.pubsub.common.PublishMessage.PublishPath;

/**
 * 受信 publish メッセージの統計情報取得用カウンタ
 */
public class TrafficCounter {
    /*--- logger ---*/
    private static final Logger logger = LoggerFactory.getLogger(TrafficCounter.class);

    private Map<String, List<LoggingElement>> log = new HashMap<String, List<LoggingElement>>();
    private long timeWindow;    // ログ情報を保持する期間 (ms)

    /**
     * 個別の受信情報保持クラス
     */
    private static class LoggingElement implements Comparable<LoggingElement> {
        private long timestamp;
        private long delay;
        private PublishPath path;

        /**
         * コンストラクタ
         * @param timestamp タイムスタンプ
         * @param delay 受信遅延時間 (ms)
         * @param path 配信経路(Overlay or OFM)
         */
        public LoggingElement(long timestamp, long delay, PublishPath path) {
            if (timestamp < 0)
                throw new IllegalArgumentException("timestamp should be positive");
            if (path == null)
                throw new NullPointerException("path should not be null");

            this.timestamp = timestamp;
            this.delay = delay;
            this.path = path;
        }

        @Override
        public int compareTo(LoggingElement e) {
            if (e == null)
                return 1;
            if (this == e)
                return 0;
            if (this.timestamp < e.timestamp)
                return -1;
            else if (this.timestamp > e.timestamp)
                return 1;
            else
                return 0;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getDelay() {
            return delay;
        }

        public PublishPath getPath() {
            return path;
        }

        @Override
        public String toString() {
            return "LoggingElement [timestamp=" + timestamp + ", delay="
                    + delay + ", path=" + path + "]";
        }
    }

    /**
     * コンストラクタ
     * @param timewindow ログ情報を保持する期間 (ms)
     */
    public TrafficCounter(long timewindow) {
        if (timewindow <= 0)
            throw new IllegalArgumentException("timewindow should be positive");

        this.timeWindow = timewindow;
    }


    /**
     * ログ情報を保持する期間を設定する
     * @param timewindow ログ情報を保持する期間 (ms)
     */
    public synchronized void setTimewindow(long timewindow) {
        this.timeWindow = timewindow;
    }

    public synchronized void addReceiveInfo(long timestamp, String topic, long delay, PublishPath publishpath) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (timestamp < 0)
            throw new IllegalArgumentException("timestamp should be positive");
        if (publishpath == null)
            throw new NullPointerException("publishPath should not be null");

        List<LoggingElement> logs = log.get(topic);
        if (logs == null) {
            logs = new ArrayList<LoggingElement>();
            log.put(topic, logs);
        }
        logs.add(new LoggingElement(timestamp, delay, publishpath));
        long limittime = System.currentTimeMillis() - timeWindow;
        removeTimeoutElement(topic, limittime);
    }

    /**
     * 時刻 limittime より古い履歴を削除する
     * @param topic 
     * @param limittime 
     */
    private void removeTimeoutElement(String topic, long limittime) {
        if (!log.containsKey(topic))
            return;
        List<LoggingElement> logs = log.get(topic);
        // 保持期間から外れる履歴を削除
        if (!logs.isEmpty()) {
            LoggingElement head = logs.get(0);
            while (head.getTimestamp() < limittime) {
                logs.remove(0);
                logger.trace("remove {}", head);
                if (logs.isEmpty())
                    break;
                head = logs.get(0);
            }
        }
    }

    /**
     * topic に対する受信時統計情報を取得する
     * @param topic
     * @return 受信時統計情報
     */
    public synchronized TrafficStatistics getStatistics(String topic) {
        if (topic == null)
            throw new NullPointerException("topic should not be null");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic should not be empty");
        if (!log.containsKey(topic))
            return new TrafficStatistics(0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0, 0.0);

        long limittime = System.currentTimeMillis() - timeWindow;
        removeTimeoutElement(topic, limittime);

        List<LoggingElement> logs = log.get(topic);
        logger.trace("Log size {}", logs.size());

        if (logs.isEmpty())
            return new TrafficStatistics(0, 0.0, 0.0, 0, 0.0, 0.0, 0, 0.0, 0.0);

        long delay_sum = 0;
        long overlay_msg = 0;
        long overlay_delay_sum = 0;
        long ofm_msg = 0;
        long ofm_delay_sum = 0;

        // メッセージ数と遅延時間の平均を算出
        for (LoggingElement log : logs) {
            delay_sum += log.getDelay();
            if (log.getPath() == PublishPath.Overlay) {
                overlay_msg++;
                overlay_delay_sum += log.getDelay();
            }
            if (log.getPath() == PublishPath.OFM) {
                ofm_msg++;
                ofm_delay_sum += log.getDelay();
            }
        }

        double delay_avg = (double) delay_sum / logs.size();
        double overlay_delay_avg = 
                (overlay_msg == 0) ? 0.0 : (double) overlay_delay_sum / overlay_msg;
        double ofm_delay_avg = 
                (ofm_msg == 0) ? 0.0 : (double) ofm_delay_sum / ofm_msg;

        // 遅延時間の分散を算出
        double delay_div = 0.0;
        double overlay_delay_div = 0.0;
        double ofm_delay_div = 0.0;
        for (LoggingElement log : logs) {
            double diff = (double) log.getDelay() - delay_avg;
            delay_div += (diff * diff);
            if (log.getPath() == PublishPath.Overlay) {
                diff = (double) log.getDelay() - overlay_delay_avg;
                overlay_delay_div += (diff * diff);
            }
            if (log.getPath() == PublishPath.OFM) {
                diff = (double) log.getDelay() - ofm_delay_avg;
                ofm_delay_div += (diff * diff);
            }
        }
        delay_div = delay_div / logs.size();
        overlay_delay_div = (overlay_msg == 0) ? 0.0 : overlay_delay_div / overlay_msg;
        ofm_delay_div = (ofm_msg == 0) ? 0.0 : ofm_delay_div / ofm_msg;

        System.out.println("統計情報:メッセージ数" + logs.size() +" 遅延平均"+ delay_avg +" 遅延分散"+ delay_div +" ALMメッセージ"+ 
                overlay_msg +" ALM遅延"+ overlay_delay_avg +" ALM分散"+ overlay_delay_div +" OFMメッセージ"+
                ofm_msg +" OFM遅延"+ ofm_delay_avg +" OFM分散"+ ofm_delay_div);
        return new TrafficStatistics(logs.size(), delay_avg, delay_div, 
                overlay_msg, overlay_delay_avg, overlay_delay_div,
                ofm_msg, ofm_delay_avg, ofm_delay_div);
    }
}
