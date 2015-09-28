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


public class HBox implements Comparable<HBox>{

    private final String code;

    public HBox() {
        this.code = "";
    }

    public HBox(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public HBox getUpperHalf() {
        return new HBox(code + "1");
    }

    public HBox getLowerHalf() {
        return new HBox(code + "0");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HBox)) return false;

        HBox hBox = (HBox) o;

        if (code != null ? !code.equals(hBox.code) : hBox.code != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return code != null ? code.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "HBox{" +
                "code='" + code + '\'' +
                '}';
    }

    @Override
    public int compareTo(HBox o) {
        return o.getCode().compareTo(this.code);
    }
}