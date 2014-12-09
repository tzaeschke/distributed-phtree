package ch.ethz.globis.distindex.mapping.zorder;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.BST;
import ch.ethz.globis.pht.PhTreeRangeVD;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * Mapping for the Z-Order curve.
 */
public class ZMapping implements KeyMapping<long[]>{

    int dim;
    private ZOrderService service;
    private boolean consistent = true;
    private PhTreeRangeVD<String> tree;
    private Map<String, Integer> sizes;
    private Map<String, Integer> order;

    /**
     * No-arg constructor, needed for Kryo deserialization.
     * Should not be called.
     */
    public ZMapping() {
    }

    public ZMapping(int dim, int depth) {
        this.dim = dim;
        this.service = new ZOrderService(depth);
        this.tree = new PhTreeRangeVD<>(dim);
        this.sizes = new TreeMap<>();
        this.order = new TreeMap<>();
    }

    /**
     * Add multiple host id's at once.
     *
     *  @param hostIds
     */
    public void add(List<String> hostIds) {
        String[] hosts = hostIds.toArray(new String[hostIds.size()]);
        BST bst = BST.fromArray(hosts);

        updateRegions(bst.asMap());
    }

    /**
     * Add the current host id to the mapping. Following the addition, the new host should also
     * be mapped to an interval on the z-order curve.
     *
     *
     * @param hostId                                    The id of the new host.
     * @return                                          The mapping as a Map object. Each entry contains the prefix
     *                                                  as the key and the hostId as the value.
     */
    public void add(String hostId) {
        checkConsistency();

        //add the nest hostId to the mapping
        Map<String, String> mapping = constructNewMapping(hostId);

        //reconstruct the set of regions mapped to each host
        updateRegions(mapping);
    }

    /**
     * Add the new host to the mapping.
     * @param newHostId
     * @return
     */
    private Map<String, String> constructNewMapping(String newHostId) {
        Set<String> hosts = sizes.keySet();
        BST bst = new BST();
        for (String host : hosts) {
            bst.add(host);
        }
        bst.add(newHostId);
        Map<String, String> mapping = bst.asMap();
        return mapping;
    }

    /**
     * Reconstruct the set of hquad regions associated with each host
     * based on the new mapping.
     *
     * @param mapping
     */
    private void updateRegions(Map<String, String> mapping) {
        tree = new PhTreeRangeVD<>(dim);
        String prefix, host;
        int orderCount = 0;
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            prefix = entry.getKey();
            host = entry.getValue();

            //for each host
            long[] start = service.generateRangeStart(prefix, dim);
            long[] end = service.generateRangeEnd(prefix, dim);
            tree.put(convert(start), convert(end), host);
            order.put(host, orderCount++);
            sizes.put(host, 1);
        }
    }

    public void remove(String hostId) {
        this.consistent = false;
    }

    /**
     * Return the host id mapped to the zone of which the key k belongs to.
     *
     * @param k                                 The input key.
     * @return
     */
    public String get(long[] k) {
        checkConsistency();

        double[] key = convert(k);

        PhTreeRangeVD<String>.PHREntryIterator it = tree.queryIntersect(key, key);
        double[] start;
        double[] end;
        PhTreeRangeVD.PHREntry entry;

        String host = null;
        if (it.hasNext()) {
            entry = it.next();
            start = entry.lower();
            end = entry.upper();
            if (it.hasNext()) {
                throw new IllegalStateException("Areas not overlapping, more intersections returned.");
            }
            host = getValueFromTree(start, end);
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

        double[] lower = convert(l);
        double[] upper = convert(u);

        PhTreeRangeVD<String>.PHREntryIterator it = tree.queryIntersect(lower, upper);
        List<String> unsortedHosts = new ArrayList<>();
        String host;
        PhTreeRangeVD.PHREntry e;
        while (it.hasNext()) {
            e = it.next();
            host = getValueFromTree(e.lower(), e.upper());
            unsortedHosts.add(host);
        }

        //ToDO sort the hosts according to the z-order
        return unsortedHosts;
    }

    private String getValueFromTree(double[] lower, double[] upper) {
        String host = tree.put(lower, upper, null);
        if (host != null) {
            tree.put(lower, upper, host);
            return host;
        }
        return null;
    }

    @Override
    public void setSize(String host, int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> get() {
        return new ArrayList<>(order.keySet());
    }

    @Override
    public String getFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNext(String hostId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * Serialize the current mapping object into an array of bytes.
     *
     * @return                                  The array of bytes.
     */
    public byte[] serialize() {
        Kryo kryo = new Kryo();
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, this);
        return output.toBytes();
    }

    /**
     * Create a mapping object from a serialized representation of a mapping.
     *
     * @param data                              The byte array obtained by serializing a previous mapping.
     * @return                                  A mapping object.
     */
    public static ZMapping deserialize(byte[] data) {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return (ZMapping) kryo.readClassAndObject(new Input(data));
    }

    private void checkConsistency() {
        if (!consistent) {
            throw new IllegalStateException("Mapping is inconsistent!");
        }
    }

    private double[] convert(long[] key) {
        double[] d = new double[key.length];
        for (int i = 0; i < d.length; i++) {
            d[i] = key[i];
        }
        return d;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZMapping)) return false;

        ZMapping mapping = (ZMapping) o;

        if (consistent != mapping.consistent) return false;
        if (dim != mapping.dim) return false;
        if (order != null ? !order.equals(mapping.order) : mapping.order != null) return false;
        if (service != null ? !service.equals(mapping.service) : mapping.service != null) return false;
        if (sizes != null ? !sizes.equals(mapping.sizes) : mapping.sizes != null) return false;

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
        result = 31 * result + (sizes != null ? sizes.hashCode() : 0);
        result = 31 * result + (order != null ? order.hashCode() : 0);
        return result;
    }
}