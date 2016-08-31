// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.jobs.stores

import com.yahoo.bard.webservice.async.jobs.stores.JobRowFilter
import com.yahoo.bard.webservice.web.BadFilterException
import com.yahoo.bard.webservice.web.FilterOperation

import spock.lang.Specification
import spock.lang.Unroll

class JobRowFilterSpec extends Specification {

    @Unroll
    def "Good ApiJobStore filter query #jobField-#op#values parses correctly"() {
        given:
        String query = "$jobField-$op$values"

        when:
        JobRowFilter jobRowFilter = new JobRowFilter(query)

        then:
        jobRowFilter.jobField?.name == jobField
        jobRowFilter.operation == FilterOperation.valueOf(op)
        jobRowFilter.values == expected as Set

        where:
        jobField  | op           | values           | expected
        'userId'  | 'eq'         | '[foo,bar,baz]'  | ['foo', 'bar', 'baz']
        'userId'  | 'eq'         | '[foo]'          | ['foo']
        'userId'  | 'in'         | '[foo,bar,baz]'  | ['foo', 'bar', 'baz']
        'userId'  | 'notin'      | '[foo]'          | ['foo']
        'userId'  | 'startswith' | '[fo]'           | ['fo']
        'userId'  | 'contains'   | '[oo]'           | ['oo']

    }

    @Unroll
    def "Bad ApiJobStore filter query #query throws #exception.simpleName because #reason"() {
        when:
        new JobRowFilter(query)

        then:
        thrown exception

        where:
        query                     | exception          | reason
        'unknown-eq[foo]'         | BadFilterException | 'Unknown JobField'
        'userId-unknown[foo]'     | BadFilterException | 'Unknown Operation'
        'userIdeq[foo]'           | BadFilterException | 'Missing Dash'
        'userId-eqfoo]'           | BadFilterException | 'Missing Opening Bracket'
        'userId-eq[]'             | BadFilterException | 'Missing value list elements'
        'userId-[foo]'            | BadFilterException | 'Missing Operation'
        '-eq[f00]'                | BadFilterException | 'Missing JobField'
        'userId-eq'               | BadFilterException | 'Missing value list'

        'unknown-eq[foo,bar]'     | BadFilterException | 'Unknown JobField (multi-value)'
        'userId-unknown[foo,bar]' | BadFilterException | 'Unknown Operation (multi-value)'
        'userIdeq[foo,bar]'       | BadFilterException | 'Missing Dash (multi-value)'
        'userId-eqfoo,bar]'       | BadFilterException | 'Missing Opening Bracket (multi-value)'
        'userId-[foo,bar]'        | BadFilterException | 'Missing Operation (multi-value)'
        '-eq[foo,bar]'            | BadFilterException | 'Missing JobField (multi-value)'

        'userId-eq[,bar]'         | BadFilterException | 'Filter requests empty string'
        'userId-eq[foo,]'         | BadFilterException | 'Filter requests empty string'
        'userId-eq[f,,b]'         | BadFilterException | 'Filter requests empty string'
        'userId-eq[f, ,b]'        | BadFilterException | 'Filter requests empty string'
        'userId-eq[,]'            | BadFilterException | 'Filter requests empty string'
        'userId-eq[,,]'           | BadFilterException | 'Filter requests empty string'
        'userId-eq[ ]'            | BadFilterException | 'Filter requests empty string'
        'userId-eq[  ]'           | BadFilterException | 'Filter requests empty string'
        'userId-eq[]'             | BadFilterException | 'Filter requests empty string'
    }
}
