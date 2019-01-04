// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.web.apirequest.binders.DefaultGranularityBinder;
import com.yahoo.bard.webservice.web.apirequest.binders.DefaultTableBinder;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.joda.time.DateTimeZone;

import java.util.List;

import javax.ws.rs.core.PathSegment;

/**
 * An implementation of DataApiRequestFactory that does not modify the initial parameters at all.
 */
public class DefaultDataApiRequestValueObjectFactory implements DataApiRequestFactory {

    DefaultTableBinder defaultTableBinders = new DefaultTableBinder();
    DefaultGranularityBinder defaultGranularityBinder = new DefaultGranularityBinder();

    @Override
    public DataApiRequest buildApiRequest(
            String tableName,
            String granularityName,
            List<PathSegment> dimensions,
            String logicalMetricNames,
            String intervalNames,
            String apiFilterNames,
            String havings,
            String sorts,
            String count,
            String topN,
            String format,
            String downloadFilename,
            String timeZoneId,
            String asyncAfter,
            String perPage,
            String page,
            BardConfigResources bardConfigResources
    ) {
        DateTimeZone dateTimeZone = defaultGranularityBinder.bindTimeZone(
                timeZoneId,
                bardConfigResources.getSystemTimeZone()
        );
        Granularity granularity = defaultGranularityBinder.bindGranularity(
                granularityName,
                dateTimeZone,
                bardConfigResources.getGranularityParser()
        );
        DataApiRequestVauleObjectImpl result = new DataApiRequestVauleObjectImpl();
        result = result.withGranularity(granularity)
                .withTable(defaultTableBinders.bindLogicalTable(
                        tableName,
                        granularity,
                        bardConfigResources.getLogicalTableDictionary()
                ));
        //TODO lots of stuff
        return result;
    }
}
