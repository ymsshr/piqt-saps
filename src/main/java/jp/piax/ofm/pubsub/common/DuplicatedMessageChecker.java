package jp.piax.ofm.pubsub.common;

import java.util.HashSet;
import java.util.LinkedList;

/**
 * 重複受信メッセージの検出を行う
 * 
 * PublishMessage の内、 UserID, PublishID から受信済みか否かの判定を行う
 */
public class DuplicatedMessageChecker {
    private HashSet<String> recvedIds = new HashSet<>();
    private LinkedList<String> idSequence = new LinkedList<>();
    private int capacity;

    /**
     * コンストラクタ
     * @param capacity 判定用に一時保存する受信メッセージのキュー長
     */
    public DuplicatedMessageChecker(int capacity) {
        if (capacity < 1)
            throw new IllegalArgumentException("capacity should be a positive number");

        this.capacity = capacity;
    }

    /**
     * 重複受信メッセージの判定を行う
     * 
     * 未受信の場合は判定用キューに登録され、以降の判定に用いられる
     * @param msg
     * @return true 受信済み false 未受信（キュー内に合致無し）
     */
    public synchronized boolean checkDuplicated(PublishMessage msg) {
        if (msg == null)
            throw new IllegalArgumentException("msg should not be null");

        String id = msg.getUserId() + ":" + msg.getPublishId();

        if (recvedIds.contains(id))   // キューに登録済み
            return true;

        // キューに登録
        recvedIds.add(id);
        idSequence.add(id);

        // capacity 越え分を削除
        while (capacity < recvedIds.size()) {
            String removing_id = idSequence.removeFirst();
            recvedIds.remove(removing_id);
        }
        return false;
    }
}
