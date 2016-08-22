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
package ch.ethz.globis.distindex.phtree;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ethz.globis.distindex.test.TestUtilAPIDistributed;
import ch.ethz.globis.phtree.test.TestIndexInsertion;
import ch.ethz.globis.phtree.test.TestIndexQueries;
import ch.ethz.globis.phtree.test.TestNearestNeighbour;
import ch.ethz.globis.phtree.test.TestValues;
import ch.ethz.globis.phtree.test.TestValuesD;
import ch.ethz.globis.phtree.test.util.TestUtil;

//ignored to make compilation finish faster
@Ignore
@RunWith(org.junit.runners.Suite.class)
@Suite.SuiteClasses({TestNearestNeighbour.class, TestIndexQueries.class, TestIndexInsertion.class, TestValues.class, TestValuesD.class})
public class TestSuiteSelective {

    private static final Logger LOG = LoggerFactory.getLogger(TestPhtreeSuite.class);

    static final int NUMBER_OF_SERVERS = 4;

    @BeforeClass
    public static void init() {
        try {
        	System.err.println("TestSuiteSelective is currently disabled");
            TestUtil.setTestUtil(new TestUtilAPIDistributed(NUMBER_OF_SERVERS));
        } catch (IOException e) {
            LOG.error("Failed to create the new testing utility.");
        }
    }
}
