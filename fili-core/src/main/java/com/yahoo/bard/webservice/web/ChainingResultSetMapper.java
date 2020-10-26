// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSet;
import com.yahoo.bard.webservice.data.ResultSetSchema;

import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.data.metric.mappers.RenamableResultSetMapper;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This Mapper provides ability to chain additional ResultSetMappers along with the mappers from dependent metric.
 *
 */
public class ChainingResultSetMapper  extends ResultSetMapper implements RenamableResultSetMapper {

    List<ResultSetMapper> chainedResultSetMappers;
    public static final NoOpResultSetMapper NO_OP_MAPPER = new NoOpResultSetMapper();

    /**
     * Constructor.
     *
     * @param chainedResultSetMappers  The Combined list of ResultSetMappers from dependent metric and
     *                                 additional Mappers to be chained.
     */
    public ChainingResultSetMapper(List<ResultSetMapper> chainedResultSetMappers) {
        this.chainedResultSetMappers = chainedResultSetMappers;
    }

    /**
     * Utility method to Create and Rename the ResultSetMappers in the chained List.
     *
     * @param info  LogicalMetricInfo used to rename the mapper with final metric name.
     * @param mappers  The List of mappers from dependent metrics and additional mappers to be chained in list.
     *
     * @return  The The Chained ResultSetMapper
     */
    public static ResultSetMapper createAndRenameResultSetMapperChain(LogicalMetricInfo info,
                                                                      ResultSetMapper... mappers) {
        List<ResultSetMapper> chainedResultSetMappers = Arrays.asList(mappers);

        return getRenamedMappers(chainedResultSetMappers, info.getName());
    }

    @Override
    public ResultSetMapper withColumnName(String newColumnName) {
        return getRenamedMappers(chainedResultSetMappers, newColumnName);
    }

    /**
     * This method loop through all ResultSetMappers and Rename them if necessary and
     * also ignores NoOp Mappers from the list.
     *
     * @param chainedResultSetMappers  The List of ResultSetMappers in Chain.
     * @param newColumnName  The new name of the mapper
     *
     * @return  The  Chained Renamed List of mappers.
     */
    public static ResultSetMapper getRenamedMappers(List<ResultSetMapper> chainedResultSetMappers,
                                                    String newColumnName) {
        ResultSetMapper renamedMapper;
        List<ResultSetMapper> renamedMapperList = new ArrayList<>();

        for (ResultSetMapper m : chainedResultSetMappers) {
            if (m instanceof RenamableResultSetMapper) {
                renamedMapper = ((RenamableResultSetMapper) m).withColumnName(newColumnName);
                renamedMapperList.add(renamedMapper);
            } else if (!(m instanceof NoOpResultSetMapper)) {
                renamedMapperList.add(m);
            }
        }

        return new ChainingResultSetMapper(renamedMapperList);
    }

    @Override
    protected Result map(Result result, ResultSetSchema schema) {
        //NOOP
        return null;
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        //NOOP
        return null;
    }

    /**
     * Take a complete result set and replace it with one altered according to the rules of the concrete
     *  mappers in the Chain. It delegates to the map method of corresponding mapper.
     *
     * @param resultSet  The unmapped result set
     *
     * @return The mapped result set.
     */
    @Override
    public ResultSet map(ResultSet resultSet) {
        ResultSet finalResultSet = resultSet ;

        for (ResultSetMapper m : chainedResultSetMappers) {
            finalResultSet = m.map(finalResultSet);
        }

        return finalResultSet;
    }
}
