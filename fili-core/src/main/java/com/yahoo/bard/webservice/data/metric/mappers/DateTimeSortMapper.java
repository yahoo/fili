// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection;
import com.yahoo.bard.webservice.table.Schema;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *  Mapper to sort the result set based on dateTime column sort direction.
 */
public class DateTimeSortMapper extends ResultSetMapper {

    private SortDirection direction;

    /**
     * Constructor.
     *
     * @param direction  Sort direction
     */
    public DateTimeSortMapper(SortDirection direction) {
        this.direction = direction;
    }

    /**
     *  Sorting the resultSet based on dateTime column sort direction.
     *
     * @param resultSet  The result set need to be sorted in ascending or descending order
     *
     * @return sorted ResultSet
     */
    @Override
    public ResultSet map(ResultSet resultSet) {

        Map<DateTime, List<Result>> bucketizedResultsMap = new LinkedHashMap<>();

        for (Result result: resultSet) {
            bucketizedResultsMap.computeIfAbsent(result.getTimeStamp(), ignored -> new ArrayList<>()).add(result);
        }

        List<DateTime> dateTimeList = new ArrayList(bucketizedResultsMap.keySet());

        Collections.sort(dateTimeList, direction == SortDirection.ASC ?
                Comparator.<DateTime>naturalOrder() :
                Comparator.<DateTime>reverseOrder());

        return new ResultSet(
                dateTimeList.stream()
                        .map(e -> bucketizedResultsMap.get(e))
                        .flatMap(List::stream).collect(Collectors.toList()),
                resultSet.getSchema()
        );
    }

    @Override
    protected Result map(Result result, Schema schema) {
        //Not needed, because this mapper overrides map(ResultSet). So it is just a no-op.
        return result;
    }


    @Override
    protected Schema map(Schema schema) {
        //Because this method is not necessary, it just returns the schema unchanged.
        return schema;
    }
}
