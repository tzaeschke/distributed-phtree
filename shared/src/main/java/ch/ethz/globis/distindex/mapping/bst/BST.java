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
package ch.ethz.globis.distindex.mapping.bst;

import java.util.*;

/***
 * A binary search tree implementation.
 */
public class BST {

    private BSTNode root;

    public BSTNode getRoot() {
        return root;
    }

    public void setRoot(BSTNode root) {
        this.root = root;
    }

    public void add(String host) {
        Queue<BSTNode> queue = new LinkedList<>();
        BSTNode theNewNode = newNode(host);
        if (root == null) {
            root = theNewNode;
            root.setPrefix("");
        } else {
            boolean inserted = false;
            BSTNode current;
            queue.add(root);
            while (!inserted) {
                current = queue.poll();
                if (addToNode(current, host)) {
                    inserted = true;
                } else {
                    addToQueue(queue, current.leftChild());
                    addToQueue(queue, current.rightChild());
                }
            }
            queue.clear();
        }
    }

    public boolean addToNode(BSTNode parent, String host) {
        if (parent.leftChild() == null && parent.rightChild() == null) {
            BSTNode node = new BSTNode(parent);
            node.setPrefix(parent.getPrefix() + "0");

            parent.setLeft(node);
            BSTNode newNode = newNode(host);
            newNode.setPrefix(parent.getPrefix() + "1");
            parent.setRight(newNode);

            parent.clear();
            return true;
        }
        return false;
    }

    private BSTNode newNode(String host) {
        BSTNode current = new BSTNode();
        current.setContent(host);
        return current;
    }

    private boolean addToQueue(Queue<BSTNode> queue, BSTNode node) {
        if (node != null) {
            queue.add(node);
            return true;
        }
        return false;
    }
    public List<String> getHosts(BSTNode node) {
        List<String> hosts = new ArrayList<>();
        getHosts(node, hosts);
        return hosts;
    }

    private void getHosts(BSTNode node, List<String> hosts) {
        if (node != null) {
            getHosts(node.leftChild(), hosts);
            if (node.getContent() != null) {
                hosts.add(node.getContent());
            }
            getHosts(node.rightChild(), hosts);
        }
    }

    public List<String> leaves() {
        List<String> results = new ArrayList<>();
        findRange(root, results);
        return results;
    }

    public List<BSTNode> nodes() {
        List<BSTNode> results = new ArrayList<>();
        getNodes(root, results);
        return results;
    }

    private void getNodes(BSTNode root, List<BSTNode> results) {
        if (root == null) {
            return;
        }
        if (root.getContent() != null) {
            results.add(root);
        }
        getNodes(root.leftChild(), results);
        getNodes(root.rightChild(), results);
    }

    public static BST fromArray(String[] array) {
        BSTNode node = fromArray(array, 0, array.length - 1);
        BST bst =  new BST();
        bst.setRoot(node);
        return bst;
    }

    private static BSTNode fromArray(String[] array, int start, int end) {
        if (start > end) {
            return null;
        }
        if (start == end) {
            BSTNode current = new BSTNode();
            current.setContent(array[start]);
            return current;
        }
        int mid = start + (end - start) / 2;
        BSTNode current = new BSTNode();
        current.setLeft(fromArray(array, start, mid));
        current.setRight(fromArray(array, mid + 1, end));
        return current;
    }

    /**
     * Return the first node encountered in the in-order traversal that matches
     * the content received as an arugment.
     *
     * @param content
     * @return
     */
    public BSTNode findFirstByContent(String content) {
        return findByContent(root, content);
    }

    private BSTNode findByContent(BSTNode root, String content) {
        if (root == null) {
            return null;
        }
        if (root.getContent() != null && content.equals(root.getContent())) {
            return root;
        }
        BSTNode node = findByContent(root.leftChild(), content);
        if (node != null) {
            return node;
        }
        node = findByContent(root.rightChild(), content);
        if (node != null) {
            return node;
        }
        return null;
    }

    /**
     * Return all the nodes whose content match the one received as an argument.
     *
     * If a single node matches the content, the returned list will have a single element.
     * @param content
     * @return
     */
    public List<BSTNode> findByContent(String content) {
        List<BSTNode> results = new ArrayList<>();
        findByContent(content, root, results);
        return results;
    }

    private void findByContent(String content, BSTNode root, List<BSTNode> results) {
        if (root == null) {
            return;
        }
        if (root.getContent() != null && content.equals(root.getContent())) {
            results.add(root);
        }
        findByContent(content, root.leftChild(), results);
        findByContent(content, root.rightChild(), results);
    }

    private void findRange(BSTNode current, List<String> result) {
        if (current == null) {
            return;
        }
        findRange(current.leftChild(), result);
        if (current.getContent() != null) {
            result.add(current.getContent());
        }
        findRange(current.rightChild(), result);
    }

    /**
     * Return a map containing the prefix codes of the content is each leaf node.
     *
     * @return                                      A map containing the aforementioned prefix-leaf
     *                                              mapping.
     */
    public Map<String, String> asMap() {
        Map<String, String> codeHosts = new HashMap<>();
        getHosts("", root, codeHosts);
        return codeHosts;
    }

    private void getHosts(String partial, BSTNode node, Map<String, String> map) {
        if (node != null) {
            getHosts(partial + "0", node.leftChild(), map);
            if (node.getContent() != null) {
                map.put(partial, node.getContent());
            }
            getHosts(partial + "1", node.rightChild(), map);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BST)) return false;

        BST bst = (BST) o;

        if (root != null ? !root.equals(bst.root) : bst.root != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return root != null ? root.hashCode() : 0;
    }
}