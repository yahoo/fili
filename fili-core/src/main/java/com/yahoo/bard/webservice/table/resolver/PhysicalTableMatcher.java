// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Function to test whether a Physical Table can satisfy a criteria for a request.
 */
public interface PhysicalTableMatcher extends Predicate<PhysicalTable> {

    /**
     * Determines whether this table matches a requirement.
     *
     * @param table  The table being tested
     *
     * @return True if the physical table has data which satisfies the request associated with this matcher.
     */
    boolean test(PhysicalTable table);

    /**
     * Exception to throw if no tables match.
     *
     * @return exception to raise if no tables match
     */
    NoMatchFoundException noneFoundException();

    /**
     * Run matches on each physical table in the stream, filtering down to matching tables.
     *
     * @param  tables A stream of tables to match on
     *
     * @return Any tables which match the criteria from {@link #test(PhysicalTable)}
     * @throws NoMatchFoundException if no tables in the stream match
     */
    default Set<PhysicalTable> matchNotEmpty(Stream<PhysicalTable> tables) throws NoMatchFoundException {
        Set<PhysicalTable> result = tables
                .filter(this)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (result.isEmpty()) {
            throw noneFoundException();
        }
        return result;
    }
}
