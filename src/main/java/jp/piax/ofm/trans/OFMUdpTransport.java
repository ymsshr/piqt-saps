package jp.piax.ofm.trans;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.piax.GTransConfigValues;
import org.piax.common.PeerId;
import org.piax.gtrans.NoSuchPeerException;
import org.piax.gtrans.ReceivedMessage;
import org.piax.gtrans.raw.InetLocator;
import org.piax.gtrans.raw.InetTransport;
import org.piax.gtrans.raw.RawChannel;
import org.piax.gtrans.raw.udp.UdpLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OFM UDP用のRawTransportを実現するクラス。
 * 
 * 送信元ポートを固定するため org.piax.gtrans.raw.udp.UdpTransport を元
 * に、送受信で同ソケット使うように変更
 */
public class OFMUdpTransport extends InetTransport<UdpLocator> {
    /*--- logger ---*/
    private static final Logger logger = 
        LoggerFactory.getLogger(OFMUdpTransport.class);
    
    static String THREAD_NAME_PREFIX = "ofmudp-";
    static final AtomicInteger thNum = new AtomicInteger(1);

    class SocketListener extends Thread {
        private final byte[] in = new byte[getMTU() + 1];
            // +1 しているのは、MTUを越えて受信するケースをエラーにするため
        private DatagramPacket inPac = new DatagramPacket(in, in.length);

        SocketListener() {
            super(THREAD_NAME_PREFIX + thNum.getAndIncrement());
        }
        
        @Override
        public void run() {
            while (!isTerminated) {
                try {
                    socket.receive(inPac);
                    int len = inPac.getLength();
                    OFMUdpLocator src = new OFMUdpLocator(
                            (InetSocketAddress) inPac.getSocketAddress());
                    receiveBytes(src, Arrays.copyOf(in, len));
                } catch (IOException e) {
                    if (isTerminated) break;  // means shutdown
                    logger.warn("", e);      // temp I/O error
                }
            }
        }
    }

    private final DatagramSocket socket;
    private final SocketListener socListener;
    private volatile boolean isTerminated;
    
    public OFMUdpTransport(PeerId peerId, UdpLocator peerLocator) throws IOException {
        super(peerId, peerLocator, true);
        // create socket
        int port = peerLocator.getPort();

        socket = new DatagramSocket(port, peerLocator.getInetAddress());
        socket.setSendBufferSize(GTransConfigValues.SOCKET_SEND_BUF_SIZE);
        socket.setReceiveBufferSize(GTransConfigValues.SOCKET_RECV_BUF_SIZE);
        
        System.out.println("port = " + port);
        System.out.println("IP Address = " + peerLocator.getInetAddress());
        if(socket.getLocalAddress() instanceof Inet4Address){
        	System.out.println("Using the IPv4 Socket");
        }else{
        	System.out.println("Using the IPv6 Socket");
        }
        
        // start lister thread
        isTerminated = false;
        socListener = new SocketListener();
        socListener.start();
    }

    @Override
    public void fin() {
        if (isTerminated) {
            return;     // already terminated
        }
        super.fin();
        // terminate the socListener thread
        isTerminated = true;
        socket.close();
        try {
            socListener.join();
        } catch (InterruptedException e) {}
    }

    @Override
    public int getMTU() {
        return GTransConfigValues.MAX_PACKET_SIZE;
    }

    void receiveBytes(OFMUdpLocator src, byte[] msg) {
        if (msg.length > getMTU()) {
            logger.error("receive data over MTU:" + msg.length + "bytes");
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(msg);
        ReceivedMessage rmsg = new ReceivedMessage(null, src, bb);
        if (this.listener != null)
            this.listener.onReceive(this, rmsg);
    }

    @Override
    public void send(UdpLocator toPeer, ByteBuffer bbuf) throws IOException {
        int len = bbuf.remaining();
        if (len > getMTU()) {
            logger.error("send data over MTU:" + len + "bytes");
            return;
        }
        // send
        try {
            socket.send(new DatagramPacket(bbuf.array(), len,
                    ((InetLocator) toPeer).getSocketAddress()));
        } catch (SocketException e) {
            // SocketExceptionが発生した場合は、通信先のPeerが存在しないとみなす
            throw new NoSuchPeerException(e);
        }
    }

    @Override
    public RawChannel<UdpLocator> newChannel(UdpLocator dst, boolean isDuplex,
            int timeout) throws IOException {
        throw new UnsupportedOperationException();
    }
}
