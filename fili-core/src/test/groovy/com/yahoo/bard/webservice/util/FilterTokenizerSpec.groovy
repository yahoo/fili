// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.util.FilterTokenizer

import spock.lang.Specification
import spock.lang.Unroll

class FilterTokenizerSpec extends Specification {
    @Unroll
    def "Good filter values #values parse correctly"() {
        expect:
        FilterTokenizer.split(values) == expected

        where:
        values                      | expected
        'foo'                       | ['foo']
        'foo,bar'                   | ['foo', 'bar']
        '"foo, bar and baz",qux'    | ['foo, bar and baz', 'qux']
        'foo,"",bar'                | ['foo', '', 'bar']
        '""'                        | ['']
        // Escaping refers to spock here, not the FilterTokenizer
        'foo,"2\'10"""'             | ['foo', '2\'10"']
    }

    @Unroll
    def "Bad filter values #values throw #exception.simpleName because #reason"() {
        when:
        FilterTokenizer.split(values)

        then:
        thrown exception

        where:
        values              | exception                | reason
        ',foo'              | IllegalArgumentException | 'Filter requests empty string'
        'foo,'              | IllegalArgumentException | 'Filter requests empty string'
        'foo,,bar'          | IllegalArgumentException | 'Filter requests empty string'
        'foo, ,bar'         | IllegalArgumentException | 'Filter requests empty string'
        '"",'               | IllegalArgumentException | 'Filter requests empty string'
        ',""'               | IllegalArgumentException | 'Filter requests empty string'
        ','                 | IllegalArgumentException | 'Filter requests empty string'
        ',,'                | IllegalArgumentException | 'Filter requests empty string'
        ' '                 | IllegalArgumentException | 'Filter requests empty string'
        '  '                | IllegalArgumentException | 'Filter requests empty string'
        ''                  | IllegalArgumentException | 'Filter requests empty string'
    }
}
