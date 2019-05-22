// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidSearchQuery
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import spock.lang.Specification

import java.util.concurrent.Future

class DruidDimensionValueLoaderSpec extends Specification {

    DruidWebService druidWebService = Mock(DruidWebService)
    DimensionDictionary dimensionDictionary = new DimensionDictionary()
    PhysicalTableDictionary physicalTableDictionary = new PhysicalTableDictionary()

    DruidDimensionValueLoader loader;

    def setup() {
        loader = new DruidDimensionValueLoader(physicalTableDictionary, dimensionDictionary, druidWebService)
    }


    def "Intervals move forward in time across calls to DimensionValueLoader"() {
        setup:
        Dimension dimension = Mock(Dimension)
        DataSource dataSource = Mock(DataSource)
        DruidSearchQuery searchQuery1, searchQuery2

        when:
        loader.query(dimension, dataSource)

        then:
        druidWebService.postDruidQuery(*_) >> {
            arguments ->
                searchQuery1 = arguments[4]
                return Mock(Future)
        }

        when:
        Thread.sleep(10)
        loader.query(dimension, dataSource)

        then:
        druidWebService.postDruidQuery(*_) >> {
            arguments ->
                searchQuery2 = arguments[4]
                return Mock(Future)
        }

        searchQuery2.getIntervals().get(0).getEnd() > searchQuery1.getIntervals().get(0).getEnd()
        searchQuery2.getIntervals().get(0).getStart() > searchQuery1.getIntervals().get(0).getStart()

    }
}
