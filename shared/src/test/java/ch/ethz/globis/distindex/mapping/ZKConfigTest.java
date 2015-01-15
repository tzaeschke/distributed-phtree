package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.orchestration.v2.ZKConfig;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Ignore
public class ZKConfigTest {

    @Test
    public void testConnection() {
        String hostPort = "localhost:2181";
        ZKConfig config = new ZKConfig(hostPort);
        config.start();
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            config.close();
        }
    }
}
