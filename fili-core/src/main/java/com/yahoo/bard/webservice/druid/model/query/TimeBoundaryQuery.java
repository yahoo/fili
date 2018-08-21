// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

import com.yahoo.bard.webservice.druid.model.DefaultQueryType;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;

/**
 * Druid time boundary query.
 */
public class TimeBoundaryQuery extends AbstractDruidQuery<TimeBoundaryQuery>
        implements DruidMetadataQuery<TimeBoundaryQuery> {

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     * @param context  The context
     * @param incrementQueryId  true to fork a new context and bump up the query id, or false to create an exact copy
     * of the context.
     */
    protected TimeBoundaryQuery(DataSource dataSource, QueryContext context, boolean incrementQueryId) {
        super(DefaultQueryType.TIME_BOUNDARY, dataSource, context, incrementQueryId);
    }

    /**
     * Constructor.
     *
     * @param dataSource  The datasource
     */
    public TimeBoundaryQuery(DataSource dataSource) {
        super(DefaultQueryType.TIME_BOUNDARY, dataSource, null, false);
    }

    @Override
    public TimeBoundaryQuery withDataSource(DataSource dataSource) {
        return new TimeBoundaryQuery(dataSource);
    }

    @Override
    public TimeBoundaryQuery withInnermostDataSource(DataSource dataSource) {
        return withDataSource(dataSource);
    }

    @Override
    public TimeBoundaryQuery withContext(QueryContext context) {
        return new TimeBoundaryQuery(getDataSource(), context, false);
    }
}
