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

public class BSTNode {

    private BSTNode left, right;
    private String content;
    private int size;
    private String prefix;

    public BSTNode() {
    }

    public BSTNode(String prefix, String content, int size) {
        this.prefix = prefix;
        this.content = content;
        this.size = size;
    }

    public BSTNode(BSTNode original) {
        this.content = original.getContent();
        this.size = original.getSize();
    }

    public void clear() {
        this.setContent(null);
        this.setSize(0);
    }

    public BSTNode leftChild() {
        return left;
    }

    public void setLeft(BSTNode left) {
        this.left = left;
    }

    public BSTNode rightChild() {
        return right;
    }

    public void setRight(BSTNode right) {
        this.right = right;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BSTNode)) return false;

        BSTNode node = (BSTNode) o;

        if (size != node.size) return false;
        if (content != null ? !content.equals(node.content) : node.content != null) return false;
        if (prefix != null ? !prefix.equals(node.prefix) : node.prefix != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = content != null ? content.hashCode() : 0;
        result = 31 * result + size;
        result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
        return result;
    }
}
