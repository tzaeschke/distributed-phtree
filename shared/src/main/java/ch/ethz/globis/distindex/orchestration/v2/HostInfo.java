package ch.ethz.globis.distindex.orchestration.v2;

import ch.ethz.globis.distindex.util.SerializerUtil;

public class HostInfo {

    private String hostPort;
    private int version;
    private String nextHost;
    private String prevHost;
    private long[] key;

    public HostInfo() {
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getNextHost() {
        return nextHost;
    }

    public void setNextHost(String nextHost) {
        this.nextHost = nextHost;
    }

    public String getPrevHost() {
        return prevHost;
    }

    public void setPrevHost(String prevHost) {
        this.prevHost = prevHost;
    }

    public long[] getKey() {
        return key;
    }

    public void setKey(long[] key) {
        this.key = key;
    }

    public byte[] serialize() {
        return SerializerUtil.getInstance().serialize(this);
    }

    public static HostInfo deserialize(byte[] data) {
        return SerializerUtil.getInstance().deserialize(data);
    }
}