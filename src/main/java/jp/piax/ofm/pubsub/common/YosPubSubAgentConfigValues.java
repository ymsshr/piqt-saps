package jp.piax.ofm.pubsub.common;

/**
 * YosPubSubAgent 特有の設定値
 */
public class YosPubSubAgentConfigValues {
    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでの基本ウエイト時間のデフォルト値 (ms)
     */
    public static final long DEFAULT_OFM_MIGRATION_DELAY_BASE = 500;
    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでのランダムウエイト時間のデフォルト値 (ms)
     */
    public static final long DEFAULT_OFM_MIGRATION_DELAY_WIDTH = 5*1000;

    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでのウエイト
     * migrationBaseDelay + rnd(migrationAdditionalDelayWidth) ms 後に移行する
     */
    public static long OFMtopicMigrationDelayBase = DEFAULT_OFM_MIGRATION_DELAY_BASE;
    /** OFM 経由でメッセージを受け取ってから OFM topic に移行するまでのウエイト
     * migrationBaseDelay + rnd(migrationAdditionalDelayWidth) ms 後に移行する
     */
    public static long OFMtopicMigrationDelayAdditionalWidth = DEFAULT_OFM_MIGRATION_DELAY_WIDTH;

}
