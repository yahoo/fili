// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.function.Predicate;

/**
 * A predicate to determine whether a table aligns with a criteria.
 */
@FunctionalInterface
public interface IsTableAligned extends Predicate<PhysicalTable> {

    /**
     * Test for alignment between a table and a criteria.
     *
     * @param table  The table whose alignment is under test
     *
     * @return True if the table is aligned to the criteria of this predicate
     */
    boolean test(PhysicalTable table);
}
