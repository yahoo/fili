// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.jobs.jobrows.DefaultJobField;
import com.yahoo.bard.webservice.async.jobs.stores.HashJobStore;
import com.yahoo.bard.webservice.async.preresponses.stores.HashPreResponseStore;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobField;
import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore;
import com.yahoo.bard.webservice.table.Schema;
import com.yahoo.bard.webservice.table.ZonedSchema;
import com.yahoo.bard.webservice.web.PreResponse;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext;
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContextKeys;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedHashMap;

/**
 * A resource class to hold all the objects for testing the JobsEndpoint.
 */
public class JobsEndpointResources {

    /**
     * Get an instance of ApiJobStore for testing.
     *
     * @return An instance of ApiJobStore
     */
    public static ApiJobStore getApiJobStore() {
        ApiJobStore apiJobStore = new HashJobStore();

        Map<JobField, String> fieldValueMap1 = new HashMap<>();
        fieldValueMap1.put(DefaultJobField.JOB_TICKET, "ticket1");
        fieldValueMap1.put(DefaultJobField.DATE_CREATED, "2016-01-01");
        fieldValueMap1.put(DefaultJobField.DATE_UPDATED, "2016-01-01");
        fieldValueMap1.put(DefaultJobField.QUERY, "https://localhost:9998/v1/data/QUERY");
        fieldValueMap1.put(DefaultJobField.STATUS, "success");
        fieldValueMap1.put(DefaultJobField.USER_ID, "momo");

        JobRow jobRow1 = new JobRow(DefaultJobField.JOB_TICKET, fieldValueMap1);
        apiJobStore.save(jobRow1);

        Map<JobField, String> fieldValueMap2 = new HashMap<>();
        fieldValueMap2.put(DefaultJobField.JOB_TICKET, "ticket2");
        fieldValueMap2.put(DefaultJobField.DATE_CREATED, "2016-01-01");
        fieldValueMap2.put(DefaultJobField.DATE_UPDATED, "2016-01-01");
        fieldValueMap2.put(DefaultJobField.QUERY, "https://localhost:9998/v1/data/QUERY");
        fieldValueMap2.put(DefaultJobField.STATUS, "pending");
        fieldValueMap2.put(DefaultJobField.USER_ID, "dodo");

        JobRow jobRow2 = new JobRow(DefaultJobField.JOB_TICKET, fieldValueMap2);
        apiJobStore.save(jobRow2);

        return apiJobStore;
    }

    /**
     * Get an instance of PreResponseStore for testing.
     *
     * @return An instance of PreResponseStore.
     */
    public static PreResponseStore getPreResponseStore() {
        PreResponseStore preResponseStore = new HashPreResponseStore();
        Granularity granularity = AllGranularity.INSTANCE;
        Schema schema = new ZonedSchema(granularity, DateTimeZone.UTC);

        Map<MetricColumn, Object> metricValues = new HashMap<>();
        metricValues.put(MetricColumn.addNewMetricColumn(schema, "pageViews"), new BigDecimal(111));

        Result result = new Result(new HashMap<>(), metricValues, DateTime.parse("2016-01-12T00:00:00.000Z"));
        List<Result> results = new ArrayList<>();
        results.add(result);
        ResultSet resultSet = new ResultSet(results, schema);

        HashSet<String> apiMetricColumnNames = new HashSet<>();
        apiMetricColumnNames.add("pageViews");

        ResponseContext responseContext = new ResponseContext();
        responseContext.put("headers", new MultivaluedHashMap<>());
        responseContext.put("apiMetricColumnNames", apiMetricColumnNames);
        responseContext.put("requestedApiDimensionFields", new HashMap<>());

        PreResponse preResponse = new PreResponse(resultSet, responseContext);
        preResponseStore.save("ticket1", preResponse);

        preResponseStore.save("IExistOnlyInPreResponseStore", preResponse);

        ResponseContext errorResponseContext = new ResponseContext();
        errorResponseContext.put("headers", new MultivaluedHashMap<>());
        errorResponseContext.put(ResponseContextKeys.STATUS.getName(), 500);
        errorResponseContext.put(ResponseContextKeys.ERROR_MESSAGE.getName(), "Error");
        errorResponseContext.put("apiMetricColumnNames", apiMetricColumnNames);
        errorResponseContext.put("requestedApiDimensionFields", new HashMap<>());
        PreResponse errorPresResponse = new PreResponse(resultSet, errorResponseContext);
        preResponseStore.save("errorPreResponse", errorPresResponse);

        return preResponseStore;
    }
}
