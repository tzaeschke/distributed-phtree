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

import ch.ethz.globis.distindex.test.TestUtilAPIDistributed;
import ch.ethz.globis.pht.test.util.TestSuite;
import ch.ethz.globis.pht.test.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Do not execute the full test-suite because of the HighDimension test.
 */
@Ignore
public class TestPhtreeSuite extends TestSuite {

    private static final Logger LOG = LoggerFactory.getLogger(TestPhtreeSuite.class);

    static final int NUMBER_OF_SERVERS = 4;

    @BeforeClass
    public static void init() {
        try {
            TestUtil.setTestUtil(new TestUtilAPIDistributed(NUMBER_OF_SERVERS));
        } catch (IOException e) {
            LOG.error("Failed to create the new testing utility.");
        }
    }
}