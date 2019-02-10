// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.mappers;

import com.yahoo.bard.webservice.data.Result;
import com.yahoo.bard.webservice.data.ResultSetSchema;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A ResultSetMapper that transforms a ResultSet by transforming its {@link DimensionRow}s.
 *
 * If memoization is used, transformed rows will be cached.  The cost of memoization is proportional to the hashcode
 * cost of the DimensionRow entries and the value is proportional to the number of repeated rows and the cost of
 * transforming them.  In general, the expectation is for high redundancy of dimension rows per result set, so
 * memoization is on by default.
 */
public class MemoizingDimensionMappingResultSetMapper extends ResultSetMapper {

    private final BiPredicate<DimensionColumn, DimensionRow> columnRowMatcher;
    private final BiFunction<DimensionColumn, DimensionRow, Map.Entry<DimensionColumn, DimensionRow>> columnRowMapper;

    private Map<Map.Entry<DimensionColumn, DimensionRow>, Map.Entry<DimensionColumn, DimensionRow>> entryMemo;

    private final Function<Map.Entry<DimensionColumn, DimensionRow>, Map.Entry<DimensionColumn, DimensionRow>>
            entryMapper;

    /**
     * Constructor.
     * Default memoize to true.
     *
     * @param matcher  The function identifying which dimension rows should be transformed.
     * @param mapper  The function which builds a DimensionEntry from a matched DimensionRow
     */
    public MemoizingDimensionMappingResultSetMapper(
            BiPredicate<DimensionColumn, DimensionRow> matcher,
            BiFunction<DimensionColumn, DimensionRow, Map.Entry<DimensionColumn, DimensionRow>> mapper
    ) {
        this(matcher, mapper, true);
    }

    /**
     * Constructor.
     *
     * @param matcher  The function identifying which dimension rows should be transformed.
     * @param mapper  The function which builds a DimensionEntry from a matched DimensionRow
     * @param memoize  cache transformed results if appropriate
     */
    public MemoizingDimensionMappingResultSetMapper(
            BiPredicate<DimensionColumn, DimensionRow> matcher,
            BiFunction<DimensionColumn, DimensionRow, Map.Entry<DimensionColumn, DimensionRow>> mapper,
            boolean memoize
    ) {
        columnRowMatcher = matcher;
        columnRowMapper = mapper;

        if (memoize) {
            entryMemo = new HashMap<>();
            entryMapper = (entry) -> {
                if (!entryMemo.containsKey(entry)) {
                    Map.Entry<DimensionColumn, DimensionRow> value = mapEntry(entry);
                    entryMemo.put(entry, value);
                }
                return entryMemo.get(entry);
            };
        } else {
            entryMapper = this::mapEntry;
        }
    }

    /**
     * Build a mapper based on a simple field transformer.
     *
     * @param dimensionToMap  A target dimension to transform
     * @param fieldMapper  A value transformer for dimension fields
     *
     * @return A result set mapper that replaces certain field values on the dimension.
     */
    public static MemoizingDimensionMappingResultSetMapper buildFromFieldMapper(
            Dimension dimensionToMap,
            BiFunction<DimensionField, String, String> fieldMapper
    ) {
        BiPredicate<DimensionColumn, DimensionRow> entryMatcher =
                (dimensionColumn, dimensionRow) -> dimensionColumn.getDimension().equals(dimensionToMap);

        BiFunction<DimensionColumn, DimensionRow, Map.Entry<DimensionColumn, DimensionRow>> entryMapper =
                (dimensionColumn, dimensionRow) -> {
                    DimensionRow newRow = DimensionRow.copyWithReplace(dimensionRow, fieldMapper);
                    return new AbstractMap.SimpleEntry<>(dimensionColumn, newRow);
                };

        return new MemoizingDimensionMappingResultSetMapper(entryMatcher,  entryMapper, true);
    }


    @Override
    protected Result map(Result result, ResultSetSchema schema) {
        return new Result(mapDimensions(result), result.getMetricValues(), result.getTimeStamp());
    }

    /**
     * Transform the dimension portion of a result to another collection of dimensions.
     *
     * @param result  The result being transformed.
     *
     * @return  A map of columns and values representing the dimensional part of a Result.
     */
    protected Map<DimensionColumn, DimensionRow> mapDimensions(Result result) {
        return result.getDimensionRows().entrySet().stream()
                .map(entryMapper)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Transform an entry from a stream of results into an altered entry, if the entry matches the matching predicate.
     *
     * @param columnRow The column and row to potentially be altered.
     *
     * @return  The original column,row entry if the matcher returns false, otherwise the transformed one.
     */
    protected Map.Entry<DimensionColumn, DimensionRow> mapEntry(Map.Entry<DimensionColumn, DimensionRow> columnRow) {
        if (columnRowMatcher.test(columnRow.getKey(), columnRow.getValue())) {
            return columnRowMapper.apply(columnRow.getKey(), columnRow.getValue());
        } else {
            return columnRow;
        }
    }

    @Override
    protected ResultSetSchema map(ResultSetSchema schema) {
        return schema;
    }
}
