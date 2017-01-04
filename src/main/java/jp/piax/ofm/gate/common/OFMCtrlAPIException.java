package jp.piax.ofm.gate.common;

import java.io.IOException;

/**
 * OFM 設定 WebAPI へのアクセスで生じる例外
 */
public class OFMCtrlAPIException extends IOException {
    private static final long serialVersionUID = 1L;

    public OFMCtrlAPIException() {
        super();
    }

    public OFMCtrlAPIException(String msg, Throwable t) {
        super(msg, t);
    }

    public OFMCtrlAPIException(String msg) {
        super(msg);
    }

    public OFMCtrlAPIException(Throwable t) {
        super(t);
    }
}
