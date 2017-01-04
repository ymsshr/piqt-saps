package jp.piax.ofm.trans;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.piax.common.PeerId;
import org.piax.gtrans.raw.RawTransport;
import org.piax.gtrans.raw.udp.UdpLocator;

/**
 * OFM UDP用のPeerLocatorを表現するクラス。
 * newRawTransport で OFMUdpTransport を返すように org.piax.gtrans.raw.udp.UdpLocator 
 * を元に修正
 */
public class OFMUdpLocator extends UdpLocator {
    private static final long serialVersionUID = 1L;

    public OFMUdpLocator(UdpLocator udp) {
        this(udp.getSocketAddress());
    }

    public OFMUdpLocator(InetSocketAddress addr) {
        super(addr);
    }

    @Override
    public RawTransport<UdpLocator> newRawTransport(PeerId peerId)
            throws IOException {
        return new OFMUdpTransport(peerId, this);
    }

}
