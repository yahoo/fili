// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.HOURLY
import static com.yahoo.bard.webservice.data.config.names.TestDruidTableName.MONTHLY

import com.yahoo.bard.webservice.web.endpoints.BaseDataServletComponentSpec
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import org.joda.time.DateTime
import org.joda.time.Interval

/**
 * This tests verifies that when we have two tables with an equal amount of availability (both are missing the leading
 * time bucket), then table selection favors the table with more volatile data available (in the case of this
 * test, the hourly table).
 *
 * To perform this test, a query is performed against the monthly_hourly logical table, which is backed by both
 * a monthly and an hourly table (where the hourly table has more volatile data available).
 *
 * To see what date ranges are volatile for the hourly and monthly physical tables, see the implementation of
 * {@link com.yahoo.bard.webservice.application.TestBinderFactory#getVolatileIntervalsService}.
 */
class VolatilityTableSelectionSpec extends BaseDataServletComponentSpec {

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        "data/hourly_monthly/month"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        return [
                "metrics": ["limbs"],
                "dateTime": ["2016-06-01/2016-09-01"]
        ]
    }

    @Override
    void populatePhysicalTableAvailability() {
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(
                getJtb(),
                new Interval("2014-01-01/2016-08-15"),
                [HOURLY.asName()] as Set
        )
        AvailabilityTestingUtils.populatePhysicalTableCacheIntervals(
                getJtb(),
                new Interval("2014-01-01/2016-08-01"),
                [MONTHLY.asName()] as Set
        )
    }

    @Override
    String getExpectedDruidQuery() {
        """
            {
                "queryType": "timeseries",
                "dataSource": {
                    "type": "table",
                    "name": "hourly"
                },
                "granularity": {
                    "type": "period",
                    "timeZone": "UTC",
                    "period": "P1M"
                },
                "intervals": [
                        "2016-06-01T00:00:00.000Z/2016-09-01T00:00:00.000Z"
                ],
                "context": {},
                "aggregations": [
                        {
                            "name": "limbs",
                            "fieldName": "limbs",
                            "type": "longSum"
                        }
                ],
                "postAggregations": []
            }
        """
    }

    @Override
    String getFakeDruidResponse() {
        """
        [
            {
                "version" : "v1",
                "timestamp": "2016-06-01T00:00.000Z",
                "result" : {
                    "limbs" : 21804117357
                }
            },
            {
                "version" : "v1",
                "timestamp": "2016-07-01T00:00.000Z",
                "result" : {
                    "limbs" : 22212797238
                }
            },
            {
                "version" : "v1",
                "timestamp": "2016-08-01T00:00.000Z",
                "result" : {
                    "limbs" : 21804117357
                }
            }
        ]
        """
    }

    @Override
    String getExpectedApiResponse() {
        """
        {
            "rows": [
                {
                    "dateTime": "2016-06-01 00:00:00.000",
                    "limbs" : 21804117357
                },
                {
                    "dateTime": "2016-07-01 00:00:00.000",
                    "limbs" : 22212797238
                },
                {
                    "dateTime": "2016-08-01 00:00:00.000",
                    "limbs" : 21804117357
                }
            ],
            "meta": {
                "missingIntervals": [
                    "2016-08-01 00:00:00.000/2016-09-01 00:00:00.000"
                ],
                "volatileIntervals": [
                    "2016-08-01 00:00:00.000/2016-09-01 00:00:00.000"
                ]
            }
        }
        """
    }
}
