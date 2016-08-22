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
package ch.ethz.globis.distindex.mapping.zorder;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.util.CollectionUtil;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.distindex.util.SerializerUtil;
import ch.ethz.globis.phtree.PhTreeSolid;

/**
 * Mapping for the Z-Order curve.
 */
public class ZMapping implements KeyMapping<long[]>{

    private static final transient Logger LOG = LoggerFactory.getLogger(ZMapping.class);

    /** The dimension of the mapping. */
    int dim;

    /** The bit width of the mapping */
    int depth;

    /** A ZOrderService instance used by this mapping */
    private ZOrderService service;

    /** Consistency flag - is true only when the mapping is consistent */
    private boolean consistent = true;

    /** A ranged PhTree containing geometrical zones from the z-mapping mapped to each
     * host*/

    private transient PhTreeSolid<String> tree;

    private Map<String, long[]> endKeys;

    /** A list of the hosts ids.*/
    private List<String> hosts = new ArrayList<>();

    private int version = 0;

    /**
     * No-arg constructor, needed for Kryo deserialization.
     * Should not be called.
     */
    public ZMapping() {
    }

    public ZMapping(int dim, int depth) {
        this(dim, depth, new ArrayList<String>());
    }

    public ZMapping(int dim, int depth, List<String> hosts) {
        this.dim = dim;
        this.depth = depth;
        //FIXME the depth is always set to 64 because the PhTree storing the rectangles only works with 64 bits
        this.service = new ZOrderService(Long.SIZE);
        this.tree = PhTreeSolid.create(dim);
        this.endKeys = new TreeMap<>();
        this.hosts = hosts;
    }

    /**
     * Add multiple host id's at once.
     *
     *  @param hostIds
     */
    public void add(List<String> hostIds) {
        checkConsistency();

//        String[] hosts = hostIds.toArray(new String[hostIds.size()]);
//        BST bst = BST.fromArray(hosts);
        //Map<String, String> map = bst.asMap();
        this.hosts.addAll(hostIds);
        //hostIds.addAll(this.hosts);

        Map<String, String> map = constructMappingEqual(hosts);
        updateRegions(map);
        //this.hosts = hostIds;

        updateTree();
    }

    /**
     * Add the current host id to the mapping. Following the addition, the new host should also
     * be mapped to an interval on the z-order curve.
     *
     *
     * @param hostId         The id of the new host.
     * @return               The mapping as a Map object. Each entry contains the prefix
     *                       as the key and the hostId as the value.
     */
    public void add(String hostId) {
        checkConsistency();

        //add the nest hostId to the mapping
//        BST bst = constructNewMapping(hostId);
//
//        Map<String, String> mapping = bst.asMap();
        //this.hosts = bst.leaves();
        this.hosts.add(hostId);
        Map<String, String> map = constructMappingEqual(hosts);
        //reconstruct the set of regions mapped to each host
        updateRegions(map);
        updateTree();
    }

    public void updateTree() {
        this.tree = PhTreeSolid.create(dim);
        long[] start, end;
        String prevHostId = null;
        for (String hostId : hosts) {
            start = getStartKey(prevHostId);
            end = endKeys.get(hostId);
            Set<HBox> regions = service.regionEnvelopeInclusive(start, end);
            for (HBox region : regions) {
                addRectangleForRegion(hostId, region, dim);
            }
            prevHostId = hostId;
        }
        this.consistent = true;
    }

    private void addRectangleForRegion(String hostId, HBox region, int dim) {
        String regionCode = region.getCode();
        long[] start, end;
        if (regionCode.length() >= dim) {
            start = service.generateRangeStart(regionCode, dim);
            end = service.generateRangeEnd(regionCode, dim);
        } else {
            int missingCharsNr = dim - regionCode.length();
            start = service.generateRangeStart(padWithChar(regionCode, '1', missingCharsNr), dim);
            end = service.generateRangeEnd(padWithChar(regionCode, '0', missingCharsNr), dim);
        }
        tree.put(start, end, hostId);
    }

    private String padWithChar(String str, char chr, int nr) {
        for (int i = 0; i < nr; i++) {
            str = str + chr;
        }
        return str;
    }

    private long[] getStartKey(String prevHostId) {
        if (prevHostId == null) {
            return getFirstStartKey(dim);
        } else {
            long[] key = endKeys.get(prevHostId);
            return MultidimUtil.next(key, depth);
        }
    }

    private long[] getFirstStartKey(int dim) {
        long[] key = new long[dim];
        Arrays.fill(key, 0L);
        return key;
    }

    /**
     * Add the new host to the mapping.
     * @param newHostId
     * @return
     */
    //TODO remove? TZ
//    private BST constructNewMapping(String newHostId) {
//        BST bst = new BST();
//        for (String host : hosts) {
//            bst.add(host);
//        }
//        bst.add(newHostId);
//        return bst;
//    }

    /**
     * Reconstruct the set of hquad regions associated with each host
     * based on the new mapping.
     *
     * @param mapping
     */
    private void updateRegions(Map<String, String> mapping) {
        tree = PhTreeSolid.create(dim);
        String prefix, host;
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            prefix = entry.getKey();
            host = entry.getValue();
            //for each host, store the end key
            long[] end = service.generateRangeEnd(prefix, dim);
            endKeys.put(host, end);
        }
    }

    /**
     * Remove a host from the mapping.
     *
     * The mapping SHOULD be marked inconsistent and should not be able to be used following this operation.
     *
     * @param hostId
     */
    public void remove(String hostId) {
        this.consistent = false;
        this.hosts.remove(hostId);
        this.endKeys.remove(hostId);
    }

    /**
     * Return the host id mapped to the zone of which the key k belongs to.
     *
     * @param k                                 The input key.
     * @return
     */
    public String get(long[] k) {
        checkConsistency();

        PhTreeSolid.PhIteratorS<String> it = tree.queryIntersect(k, k);
        PhTreeSolid.PhEntryS<String> entry;

        String host = null;
        if (it.hasNext()) {
            entry = it.nextEntry();
            if (it.hasNext()) {
                LOG.info("Zmapping: " + this);
                LOG.info("Lower: " + Arrays.toString(entry.lower()) + " , Upper: " + Arrays.toString(entry.upper()));
                while (it.hasNext()) {
                    entry = it.nextEntry();
                    LOG.info("Lower: " + Arrays.toString(entry.lower()) + " , Upper: " + Arrays.toString(entry.upper()));
                }
                throw new IllegalStateException("Areas overlapping, more intersections returned for " + Arrays.toString(k));
            }
            host = entry.value();
        }
        return host;
    }

    /**
     * Return the list of host id's whose zones intersect with the hyper-cubic range determined by the
     * parameters l and u.
     *
     * @param l
     * @param u
     * @return
     */
    public List<String> get(long[] l, long[] u) {
        checkConsistency();

        PhTreeSolid.PhIteratorS<String> it = tree.queryIntersect(l, u);

        TreeSet<String> unsortedHosts = new TreeSet<>();
        String host;
        PhTreeSolid.PhEntryS<String> e;
        while (it.hasNext()) {
            e = it.nextEntry();
            host = getValueFromTree(e.lower(), e.upper());
            unsortedHosts.add(host);
        }

        //ToDO sort the hosts according to the z-order
        return new ArrayList<>(unsortedHosts);
    }

    private String getValueFromTree(long[] lower, long[] upper) {
        String host = tree.put(lower, upper, null);
        if (host != null) {
            tree.put(lower, upper, host);
            return host;
        }
        return null;
    }

    /**
     * @return                                      A List of the host id's of the hosts in the mapping.
     */
    @Override
    public List<String> get() {
        checkConsistency();

        return hosts;
    }

    /**
     * @return                                      The host id of the first host in the mapping.
     */
    @Override
    public String getFirst() {
        checkConsistency();

        return (hosts != null && hosts.size() > 0 ) ? hosts.get(0) : null;
    }

    /**
     * Return the id of the host whose mapped region follows the region of the host whose id was received
     * as an argument.
     * @param hostId                                The preceding host's id.
     * @return                                      The next host's id.
     */
    @Override
    public String getNext(String hostId) {
        checkConsistency();

        int index = CollectionUtil.search(hosts, hostId);

        //index of the following
        index += 1;
        return (hosts.size() > index) ? hosts.get(index) : null;
    }

    @Override
    public String getPrevious(String hostId) {
        checkConsistency();

        int index = CollectionUtil.search(hosts, hostId);

        //index of the following
        index -= 1;
        return (index >= 0) ? hosts.get(index) : null;
    }

    /**
     * @return                                      The number of hosts in the mapping.
     */
    @Override
    public int size() {
        checkConsistency();

        return hosts.size();
    }

    /**
     * Clear the mapping.
     */
    @Override
    public void clear() {
        this.hosts.clear();
        this.endKeys.clear();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    public void changeIntervalEnd(String host, long[] end, String newHostId) {
        if (newHostId != null) {
            this.endKeys.put(newHostId, this.endKeys.get(host));
            addToHostsRight(host, newHostId);
        }
        this.endKeys.put(host, end);
    }

    public void changeIntervalEnd(String sourceHostId, String destHostId) {
        this.endKeys.put(destHostId, this.endKeys.get(sourceHostId));
    }

    private void addToHostsRight(String hostId, String newHostId) {
        int index = -1;
        for (int i = 0; i < hosts.size(); i++) {
            if (hosts.get(i).equals(hostId)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            throw new IllegalArgumentException("Host " + hostId + " not in the host list." + toString());
        }
        this.hosts.add(index + 1, newHostId);
    }

    /**
     * Serialize the current mapping object into an array of bytes.
     *
     * @return                                  The array of bytes.
     */
    public byte[] serialize() {
       return SerializerUtil.getInstance().serialize(this);
    }

    /**
     * Create a mapping object from a serialized representation of a mapping.
     *
     * @param data                              The byte array obtained by serializing a previous mapping.
     * @return                                  A mapping object.
     */
    public static ZMapping deserialize(byte[] data) {
        if (data.length == 0) {
            return null;
        }
        ZMapping mapping = SerializerUtil.getInstance().deserialize(data);
        mapping.updateTree();
        return mapping;
    }

    private void checkConsistency() {
        if (!consistent) {
            throw new IllegalStateException("Mapping is inconsistent!");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZMapping)) return false;

        ZMapping mapping = (ZMapping) o;

        if (consistent != mapping.consistent) return false;
        if (dim != mapping.dim) return false;
        if (service != null ? !service.equals(mapping.service) : mapping.service != null) return false;

        //FIXME need to also compare the tree
        // if (tree != null ? !tree.toString().equals(mapping.tree.toString()) : mapping.tree != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dim;
        result = 31 * result + (service != null ? service.hashCode() : 0);
        result = 31 * result + (consistent ? 1 : 0);
        result = 31 * result + (tree != null ? tree.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String str = "ZMapping: {";
        str += " version: " + this.getVersion();
        str += ", ";
        str += "Mapping: ";
        str += Arrays.toString(getFirstStartKey(dim));
        for (String host : hosts) {
            str += " "  + host + " ";
            str += Arrays.toString(endKeys.get(host));
        }
        str += "\n";
        return str;
    }

    public Map<String, String> constructMappingEqual(List<String> hosts) {
        Map<String, String> map = new LinkedHashMap<>();
        if (hosts == null || hosts.size() == 0) {
            return map;
        }
        String endCode = "";

        int nrBitsZValue = dim * depth;
        for (int i = 0; i < nrBitsZValue; i++) {
            endCode += "1";
        }
        BigInteger end = new BigInteger(endCode, 2);
        end = end.add(BigInteger.ONE);
        int nrHosts = hosts.size();
        BigInteger displacement = end.divide(new BigInteger(Integer.toString(nrHosts)));
        BigInteger lastValue = BigInteger.ZERO;
        for (int i = 0; i < nrHosts; i++) {
            if (i == 0) {
                lastValue = displacement.subtract(BigInteger.ONE);
            } else if (i == nrHosts - 1) {
                lastValue = end.subtract(BigInteger.ONE);
            } else {
                lastValue = lastValue.add(displacement);
            }
            String bitString = toBinaryString(lastValue, nrBitsZValue);
            map.put(bitString, hosts.get(i));
        }
        return map;
    }

    public String toBinaryString(BigInteger val, int bits) {
        String str = "";
        for (int bit = bits - 1; bit >= 0; bit--) {
            if (val.testBit(bit)) {
                str += "1";
            } else {
                str += "0";
            }
        }
        return str;
    }

    public static void main(String[] args) {
        ZMapping mapping = new ZMapping(5, 64);
        mapping.add("1");
        mapping.add("2");
        mapping.add("3");
        mapping.updateTree();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void addToRightOf(String freeHostId, String currentHostId) {
        addToHostsRight(currentHostId, freeHostId);
    }
}