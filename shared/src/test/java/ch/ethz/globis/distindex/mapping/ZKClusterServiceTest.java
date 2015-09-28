/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ZKClusterServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterServiceTest.class);
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";

    @Test
    public void testAddHost() {
        ClusterService<long[]> clusterService = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            clusterService  = new ZKClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            clusterService.registerHost("1");
            clusterService.registerHost("2");
            clusterService.disconnect();

            clusterService = new ZKClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            KeyMapping<long[]> mapping2 = clusterService.getMapping();
            clusterService.deregisterHost("1");
            clusterService.deregisterHost("2");
            assertEquals(0, mapping2.size());
            clusterService.disconnect();
        } catch (Exception e) {
            LOG.error("An exception occurred ", e);
        } finally {
            if (clusterService != null) {
                clusterService.disconnect();
            }
        }
    }

    @Test
    public void testFreeHosts() {
        ClusterService<long[]> freeHost1 = null, freeHost2 = null, client = null;
        final String freeHostId1 = "one";
        final String freeHostId2 = "two";
        List<String> hostIds = new ArrayList<String>() {{ add(freeHostId1);add(freeHostId2); }};
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            freeHost1 = new ZKClusterService(ZK_HOST, ZK_PORT);
            freeHost2 = new ZKClusterService(ZK_HOST, ZK_PORT);
            client = new ZKClusterService(ZK_HOST, ZK_PORT);
            startClusterServices(freeHost1, freeHost2, client);
            freeHost1.registerFreeHost(freeHostId1);
            freeHost2.registerFreeHost(freeHostId2);

            String freeHostId = client.getNextFreeHost();
            assertNotNull(freeHostId);
            assertTrue(hostIds.contains(freeHostId));
            hostIds.remove(freeHostId);

            freeHostId = client.getNextFreeHost();
            assertNotNull(freeHostId);
            assertTrue(hostIds.contains(freeHostId));
            hostIds.remove(freeHostId);

            freeHostId = client.getNextFreeHost();
            assertNull(freeHostId);


        } catch (Exception e) {
            LOG.error("An exception occurred ", e);
        } finally {
            closeClusterServices(freeHost1, freeHost2, client);
        }
    }

    @Test
    public void testRegisterHost() {
        ClusterService<long[]> reader = null, writer = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            writer = new ZKClusterService(ZK_HOST, ZK_PORT);
            writer.connect();
            reader = new ZKClusterService(ZK_HOST, ZK_PORT);
            reader.connect();

            writer.registerHost("one");
            writer.registerHost("two");
            Thread.sleep(10);
            List<String> onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(2, onlineHosts.size());

            writer.deregisterHost("one");
            onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(1, onlineHosts.size());

            writer.deregisterHost("two");
            onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(0, onlineHosts.size());
        }  catch (Exception e) {
            LOG.error("An exception occurred ", e);
            if (reader != null) {
                reader.disconnect();
            }
            if (writer != null) {
                writer.disconnect();
            }
        }
    }

    private TestingServer newZK(int port) throws Exception {
        TestingServer zk = new TestingServer(port);
        return zk;
    }

    private void startClusterServices(ClusterService<long[]>... clusterServices) {
        for (ClusterService<long[]> clusterService :  clusterServices) {
            clusterService.connect();
        }
    }

    private void closeClusterServices(ClusterService<long[]>... clusterServices) {
        for (ClusterService<long[]> clusterService :  clusterServices) {
            clusterService.disconnect();
        }
    }

}
