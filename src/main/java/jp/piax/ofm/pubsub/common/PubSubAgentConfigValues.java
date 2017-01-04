package jp.piax.ofm.pubsub.common;

import jp.piax.ofm.pubsub.monitor.PubSubMonitor;
import jp.piax.ofm.pubsub.monitor.impl.SimpleMonitor;

public class PubSubAgentConfigValues {
    // デフォルト値
    /**
     * OFM 判定周期兼 Subscriber 統計情報収集周期 (ms) のデフォルト値
     */
    public static final long DEFAULT_POLLINGPERIOD = 5*60*1000;
    /**
     * 受信時統計情報カウンタタイムウィンドウ (ms) のデフォルト値
     */
    public static final long DEFAULT_COUNTER_TIMEWINDOW = 5*60*1000;
    /**
     * 重複受診メッセージ検出用キュー長のデフォルト値
     */
    public static final int DEFAULT_DUPLICATED_MESSAGE_QUEUE_LENGTH = 30;
    /**
     * publish 前の OFM キャッシュ参照 (true:参照する false:必ず OFGate に問い合わせる)
     */
    public static final boolean DEFAULT_USE_OFMCACHE_BEFORE_PUBLISH = false;
    /**
     * OpenFlow Multicast 時のメッセージ loopback 待ちタイムアウト (ms)
     */
    public static final long DEFAULT_RECV_LOOPBACK_TIMEOUT = 1000;
    /**
     * Pub/Sub モニタリングクラスのデフォルト値
     */
    public static final Class<? extends PubSubMonitor> DEFAULT_MONITORCLASS = SimpleMonitor.class;


    // config 値
    /**
     * OFM 判定周期兼 Subscriber 統計情報収集周期 (ms)
     */
    public static long SubscriberPollingPeriod = DEFAULT_POLLINGPERIOD;
    /**
     * 受信時統計情報カウンタタイムウィンドウ (ms)
     */
    public static long TrafficCounterTimewindow = DEFAULT_COUNTER_TIMEWINDOW;
    /**
     * 重複受診メッセージ検出用キューの長さ
     */
    public static int DuplicatedMessageQueueLength = DEFAULT_DUPLICATED_MESSAGE_QUEUE_LENGTH;
    /**
     * publish 前の OFM キャッシュ参照 (true:参照する false:必ず OFGate に問い合わせる)
     */
    public static boolean UseOFMCacheBeforePublish = DEFAULT_USE_OFMCACHE_BEFORE_PUBLISH;
    /**
     * OpenFlow Multicast 時のメッセージ loopback 待ちタイムアウト (ms)
     */
    public static long RecvLoopbackTimeout = DEFAULT_RECV_LOOPBACK_TIMEOUT;
    /**
     * Pub/Sub モニタリングクラス
     */
    public static Class<? extends PubSubMonitor> MonitorClazz = DEFAULT_MONITORCLASS;
}
