// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class ApiFilterSpec extends Specification {

    DimensionDictionary dimStore
    Dimension dimension1
    Dimension dimension2
    Dimension dimension3

    def setup() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)
        DimensionField customField = Mock(DimensionField)
        customField.getName() >> "custom"
        dimensionFields.add(customField)

        dimension1 = new KeyValueStoreDimension("dimension1", "dimension1-description", dimensionFields, MapStoreManager.getInstance("dimension1"), ScanSearchProviderManager.getInstance("dimension1"))
        dimension1.setLastUpdated(new DateTime(10000))
        dimension2 = new KeyValueStoreDimension("dimension2", "dimension2-description", dimensionFields, MapStoreManager.getInstance("dimension2"), ScanSearchProviderManager.getInstance("dimension2"))
        dimension2.setLastUpdated(new DateTime(10001))
        dimension3 = new KeyValueStoreDimension("dimension3", "dimension3-description", dimensionFields, MapStoreManager.getInstance("dimension3"), ScanSearchProviderManager.getInstance("dimension3"))
        dimension3.setLastUpdated(new DateTime(10002))

        dimStore = new DimensionDictionary()
        dimStore.add(dimension1)
        dimStore.add(dimension2)
        dimStore.add(dimension3)
    }

    @Unroll
    def "Good filter #dimension|#field-#op#values parses correctly"() {

        given:
        String query = dimension + '|' + field + '-' + op + values

        when:
        ApiFilter filter = new ApiFilter(query, dimStore)

        then:
        filter.getDimension()?.getApiName() == dimension
        filter.getDimensionField() == filter.getDimension()?.getFieldByName(field)
        filter.getOperation() == FilterOperation.valueOf(op)
        filter.getValues() == expected as Set

        where:
        dimension    | field  | op      | values          | expected
        'dimension1' | 'id'   | 'eq'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension1' | 'id'   | 'in'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension1' | 'id'   | 'notin' | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension1' | 'desc' | 'eq'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension1' | 'desc' | 'in'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension1' | 'desc' | 'notin' | '[foo,bar,baz]' | ['foo', 'bar', 'baz']

        'dimension2' | 'id'   | 'eq'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension2' | 'id'   | 'in'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension2' | 'id'   | 'notin' | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension2' | 'desc' | 'eq'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension2' | 'desc' | 'in'    | '[foo,bar,baz]' | ['foo', 'bar', 'baz']
        'dimension2' | 'desc' | 'notin' | '[foo,bar,baz]' | ['foo', 'bar', 'baz']

        'dimension1' | 'id'   | 'eq'    | '[foo]'         | ['foo']
        'dimension2' | 'desc' | 'in'    | '[foo,bar]'     | ['foo', 'bar']
    }

    @Unroll
    def "Custom filter #dimension|#field-#op#values to test #desc parses correctly"() {

        given:
        String query = dimension + '|' + field + '-' + op + values

        when:
        ApiFilter filter = new ApiFilter(query, dimStore)

        then:
        filter.getDimension()?.getApiName() == dimension
        filter.getDimensionField() == filter.getDimension()?.getFieldByName(field)
        filter.getOperation() == FilterOperation.valueOf(op)
        filter.getValues() == expected as Set

        where:
        dimension    | field    | op        | values                    | expected                  | desc
        'dimension1' | 'custom' | 'eq'      | '[foo,bar,baz]'           | ['foo', 'bar', 'baz']     | 'Regular terms'
        'dimension1' | 'custom' | 'in'      | '[foo,bar,baz]'           | ['foo', 'bar', 'baz']     | 'Regular terms'
        'dimension1' | 'custom' | 'notin'   | '[foo,bar,baz]'           | ['foo', 'bar', 'baz']     | 'Regular terms'
        'dimension1' | 'custom' | 'contains'| '[foo,""]'                | ['foo', '']               | 'Empty string'
        'dimension1' | 'custom' | 'contains'| '[foo,"bar baz"]'         | ['foo', 'bar baz']        | 'Term with whitespace'
        'dimension1' | 'custom' | 'contains'| '[foo,"bar,baz"]'         | ['foo', 'bar,baz']        | 'Term with comma'
        'dimension1' | 'custom' | 'contains'| '[foo,""""]'              | ['foo', '"']              | 'Escaped double quote'
        'dimension1' | 'custom' | 'contains'| '[run,"time:120\'5"""]'   | ['run', 'time:120\'5"']   | 'Term with single quote and escaped double quote'
    }

    @Unroll
    def "Bad filter #filter throws #exception.simpleName because #reason"() {

        when:
        new ApiFilter(filter, dimStore)

        then:
        thrown exception

        where:
        filter                              | exception          | reason
        'unknown|id-in[foo]'                | BadFilterException | 'Unknown Dimension'
        'dimension1|unknown-in[foo]'        | BadFilterException | 'Unknown Field'
        'dimension1|id-unknown[foo]'        | BadFilterException | 'Unknown Operation'
        'dimension1id-in[foo]'              | BadFilterException | 'Missing Pipe'
        'dimension1|idin[foo]'              | BadFilterException | 'Missing Dash'
        'dimension1|id-infoo]'              | BadFilterException | 'Missing Opening Bracket'
        'dimension1|id-in[]'                | BadFilterException | 'Missing value list elements'
        'dimension1|-in[foo]'               | BadFilterException | 'Missing Field'
        'dimension1|id-[foo]'               | BadFilterException | 'Missing Operation'
        '|id-in[foo]'                       | BadFilterException | 'Missing Dimension'
        'dimension1|id-in'                  | BadFilterException | 'Missing value list'

        'unknown|id-in[foo,bar]'            | BadFilterException | 'Unknown Dimension (multi-value)'
        'dimension1|unknown-in[foo,bar]'    | BadFilterException | 'Unknown Field (multi-value)'
        'dimension1|id-unknown[foo,bar]'    | BadFilterException | 'Unknown Operation (multi-value)'
        'dimension1id-in[foo,bar]'          | BadFilterException | 'Missing Pipe (multi-value)'
        'dimension1|idin[foo,bar]'          | BadFilterException | 'Missing Dash (multi-value)'
        'dimension1|id-infoo,bar]'          | BadFilterException | 'Missing Opening Bracket (multi-value)'
        'dimension1|-in[foo,bar]'           | BadFilterException | 'Missing Field (multi-value)'
        'dimension1|id-[foo,bar]'           | BadFilterException | 'Missing Operation (multi-value)'
        '|id-in[foo,bar]'                   | BadFilterException | 'Missing Dimension (multi-value)'
        'dimension1|id-contains[,foo]'      | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[foo,]'      | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[v1,,v2]'    | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[v1, ,v2]'   | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[,]'         | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[,,]'        | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[ ]'         | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[  ]'        | BadFilterException | 'Filter requests empty string'
        'dimension1|id-contains[]'          | BadFilterException | 'Filter requests empty string'
//        'dimension1|id-in[foo'       | BadFilterException | 'Missing Closing Bracket' // This one's actually OK, since it ends the line
    }

    def "toString method returns a correctly formatted ApiFilter object"() {

        given:
        String query = 'dimension1|id-eq[foobar]'

        when:
        ApiFilter filter = new ApiFilter(query, dimStore)

        then:
        filter.toString() == 'dimension1|id-eq[foobar]'
    }
}
