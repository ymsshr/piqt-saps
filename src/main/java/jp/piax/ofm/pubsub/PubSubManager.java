package jp.piax.ofm.pubsub;

import java.io.IOException;

import org.piax.agent.AgentException;
import org.piax.common.attribs.IncompatibleTypeException;
import org.piax.gtrans.IdConflictException;
import org.piax.gtrans.ov.NoSuchOverlayException;

/**
 * ピア上の UserPubSub を取りまとめるマネージャ
 */
public interface PubSubManager {
    /**
     * PubSubManager を始動する
     * @throws IOException
     * @throws IdConflictException
     * @throws IllegalArgumentException
     * @throws NoSuchOverlayException
     * @throws IncompatibleTypeException
     */
    public void start() throws IOException, IdConflictException, IllegalArgumentException, NoSuchOverlayException, IncompatibleTypeException;

    /**
     * PubSubManager を停止する
     * プロセス終了前に必ず呼び出す
     */
    public void stop();

    /**
     * userid に対応する UserPubSub インタフェースを取得する
     * ピア上に 対応する UserPubSub が無い場合は生成される
     * @param userid
     * @return
     * @throws AgentException
     */
    public UserPubSub getUserPubSub(String userid) throws AgentException;
}
