package jp.piax.ofm.gate.common;

import java.net.InetSocketAddress;

/**
 * PIAX r989 では discoveryCall に対する null return が callee 側で察知できないため
 * null を返す代わりに用いる NULl object
 */
public class NullAddress extends InetSocketAddress {
    private static final long serialVersionUID = 1L;

    public static final NullAddress NULL_ADDR = new NullAddress();

    private NullAddress() {
        super("0.0.0.0", 0);
    }
}
