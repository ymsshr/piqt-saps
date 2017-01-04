package jp.piax.ofm.gate.messages;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Set;

/**
 * IP アドレス:port と MAC アドレスを持つデータクラス
 */
public class IPMACPair implements Serializable {
    private static final long serialVersionUID = 1L;

    private InetSocketAddress ipAddress;
    private byte[] macAddress;

    /**
     * コンストラクタ
     * @param ipaddress IP アドレスとポート番号
     * @param macaddress ipaddress に対応する MAC アドレス
     */
    public IPMACPair(InetSocketAddress ipaddress, byte[] macaddress) {
        if (ipaddress == null)
            throw new NullPointerException("ipaddress should not be null");
        if (ipaddress.getAddress() == null)
            throw new IllegalArgumentException("ipaddress should not be null");
        if (macaddress == null)
            throw new NullPointerException("macaddress should not be null");
        if (macaddress.length != 6)
            throw new IllegalArgumentException("macaddress should be 6 bytes");

        this.ipAddress = ipaddress;
        this.macAddress = macaddress.clone();
    }

    /**
     * IP アドレスを取得する
     * @return
     */
    public InetSocketAddress getIpAddress() {
        return ipAddress;
    }

    /**
     * MAC アドレスを取得する
     * @return
     */
    public byte[] getMacAddress() {
        return macAddress.clone();
    }

    /**
     * IPMACPair を OFGate 用の文字列形式に変換する
     * @param addr
     * @return
     */
    public static String convertToOFMAddrFormat(IPMACPair addr) {
        String s = "";
        for(byte b : addr.macAddress) {
            s += String.format("-%02X", b);
        }
        if (s.startsWith("-"))
            s = s.substring(1);

        return String.format("%s(%s):%d", addr.getIpAddress().getAddress().getHostAddress(), s, addr.getIpAddress().getPort());
    }

    /**
     * IPMACPair の Set を OFGate 用の文字列形式で結合した文字列を返す
     * @param addrs
     * @return
     */
    public static String convertToOFMAddrString(Set<IPMACPair> addrs) {
        StringBuilder buf = new StringBuilder(64 * addrs.size());
        for (IPMACPair addr : addrs) {
            buf.append(addr.getIpAddress().getAddress().getHostAddress());
            buf.append("(");
            buf.append(String.format("%02X-%02X-%02X-%02X-%02X-%02X", 
                    addr.macAddress[0], addr.macAddress[1], addr.macAddress[2],
                    addr.macAddress[3], addr.macAddress[4], addr.macAddress[5]));
            buf.append("):");
            buf.append(addr.getIpAddress().getPort());
            buf.append(", ");
        }
        return buf.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((ipAddress == null) ? 0 : ipAddress.hashCode());
        result = prime * result + Arrays.hashCode(macAddress);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IPMACPair other = (IPMACPair) obj;
        if (ipAddress == null) {
            if (other.ipAddress != null)
                return false;
        } else if (!ipAddress.equals(other.ipAddress))
            return false;
        if (!Arrays.equals(macAddress, other.macAddress))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return convertToOFMAddrFormat(this);
    }
}
