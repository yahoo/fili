//  Copyright 2017 Yahoo Inc.
//  Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.table

import com.yahoo.fili.webservice.data.config.names.TableName
import com.yahoo.fili.webservice.data.time.ZonedTimeGrain
import com.yahoo.fili.webservice.metadata.DataSourceMetadataService
import com.yahoo.fili.webservice.table.resolver.DataSourceConstraint

class TableTestUtils {

    static List buildConcreteAndConstrained(String name, ZonedTimeGrain grain, Set<Column> columns, Map<String, String> nameMap, DataSourceMetadataService service) {
        StrictPhysicalTable table = new StrictPhysicalTable(TableName.of(name), grain, columns, nameMap, service)
        DataSourceConstraint constraint = DataSourceConstraint.unconstrained(table)
        return [table, table.withConstraint(constraint)]
    }

    static ConstrainedTable buildTable(String name, ZonedTimeGrain grain, Set<Column> columns, Map<String, String> nameMap, DataSourceMetadataService service) {
        StrictPhysicalTable table = new StrictPhysicalTable(TableName.of(name), grain, columns, nameMap, service)
        DataSourceConstraint constraint = DataSourceConstraint.unconstrained(table)
        return table.withConstraint(constraint)
    }
}
