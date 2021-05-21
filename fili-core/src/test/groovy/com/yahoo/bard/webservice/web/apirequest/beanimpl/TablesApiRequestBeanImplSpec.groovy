// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.beanimpl

import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.apirequest.ApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImplSpec
import com.yahoo.bard.webservice.web.apirequest.utils.TestingApiRequestProvider
import com.yahoo.bard.webservice.web.util.BardConfigResources


class TablesApiRequestBeanImplSpec extends TablesApiRequestImplSpec{

    MetricDictionary metricDictionary = new MetricDictionary();
    TableGroup group = new TableGroup(new LinkedHashSet<PhysicalTable>(), Collections.emptySet(), Collections.emptySet())
    LogicalTable table = new LogicalTable("table1", DefaultTimeGrain.DAY, group, metricDictionary)
    LogicalTableDictionary logicalTableDictionary = new LogicalTableDictionary()
    TableIdentifier tableIdentifier = new TableIdentifier(table);

    def setup() {
        logicalTableDictionary.put(tableIdentifier, table)
    }

    @Override
    TablesApiRequestImpl buildApiRequestImpl(final String tableName, String granularity) {
        return new TablesApiRequestImpl(tableName, granularity, "json", null, "1", "1", bardConfigResources);
    }
}
