package com.yahoo.bard.webservice.druid.model.filter

import spock.lang.Specification

/**
 * Testing behavior of DimensionalFilter class.
 */
class DimensionalFilterSpec extends Specification{

    def "Constructing dimensional filter with a null dimension throws illegal argument exception"() {
        when:
        new TestDimensionalFilter(null, Mock(FilterType))

        then:
        IllegalArgumentException excpetion = thrown()
        excpetion.message == "Filter dimension 'null' does not exist."
    }
}
