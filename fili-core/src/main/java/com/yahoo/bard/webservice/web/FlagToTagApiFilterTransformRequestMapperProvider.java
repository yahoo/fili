package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DimensionsApiRequest;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.container.ContainerRequestContext;

// TODO write and refactor this entire class to not be garbage
public class FlagToTagApiFilterTransformRequestMapperProvider {

    private static RequestMapper<DataApiRequest> dataApiRequestRequestMapper;
    private static RequestMapper<DimensionsApiRequest> dimensionsApiRequestRequestMapper;
    private static RequestMapper<TablesApiRequest> tablesApiRequestRequestMapper;

    private static final Set<FilterOperation> positiveOps = Stream.of(
            DefaultFilterOperation.in,
            DefaultFilterOperation.startswith,
            DefaultFilterOperation.contains,
            DefaultFilterOperation.eq
    ).collect(Collectors.toSet());

    private static final Set<FilterOperation> negativeOps = Collections.singleton(DefaultFilterOperation.notin);

    public static RequestMapper<DataApiRequest> getDataRequestMapper(ResourceDictionaries dictionaries) {
        if (dataApiRequestRequestMapper == null) {
            synchronized (FlagToTagApiFilterTransformRequestMapperProvider.class) {
                if (dataApiRequestRequestMapper == null) {
                    dataApiRequestRequestMapper = new FlagToTagDataRequestMapper(dictionaries);
                }
            }
        }
        return dataApiRequestRequestMapper;
    }

    private static FilterOperation transformFilterOperation(
            FlagFromTagDimension dimension,
            FilterOperation op,
            Collection<String> values
    ) {
        /* requested values should really only contain a single value. If it contains both true and false values we
        need to fail the nonsensical filter, as there is no way to transform that into a meaningful api filter.
         */
        Set<String> simplifiedRequestedValues = new HashSet<>(values);
        if (simplifiedRequestedValues.size() != 1) {
            throw new BadApiRequestException(
                    String.format(
                            "Filter on dimension %s formatted incorrectly. Flag dimensions must filter on " +
                            "exactly a single flag value.",
                            dimension.getApiName()
            ));
        }
        String filterValue = simplifiedRequestedValues.iterator().next();

        // If the filter is false we have to reverse the operation because we can only filter on the tag value
        FilterOperation newOp = op;
        if (filterValue.equalsIgnoreCase(dimension.getFalseValue())) {
            if (positiveOps.contains(op)) {
                newOp = DefaultFilterOperation.notin;
            } else if (negativeOps.contains(op)) {
                newOp = DefaultFilterOperation.in;
            } else {
                throw new BadApiRequestException(
                        String.format(
                                "Dimension %s doesn't support the operation %s. Try using one of the " +
                                        "following operations: %s",
                                dimension.getApiName(),
                                op,
                                String.join(
                                        ",",
                                        Stream.of(
                                                positiveOps,
                                                negativeOps
                                        ).flatMap(Set::stream).map(FilterOperation::getName).collect(Collectors.toSet())
                                )
                        )
                );
            }
        }
        return newOp;
    }

    private static class FlagToTagDataRequestMapper extends RequestMapper<DataApiRequest> {
        /**
         * Constructor.
         *

         * @param resourceDictionaries  The dictionaries to use for request mapping.
         */
        public FlagToTagDataRequestMapper(
                final ResourceDictionaries resourceDictionaries
        ) {
            super(resourceDictionaries);
        }

        @Override
        public DataApiRequest apply(DataApiRequest request, ContainerRequestContext context)
                throws RequestValidationException {
            ApiFilters newFilters = new ApiFilters();
            for (Map.Entry<Dimension, Set<ApiFilter>> entry : request.getApiFilters().entrySet()) {
                if (!(entry.getKey() instanceof FlagFromTagDimension)) {
                    newFilters.put(entry.getKey(), entry.getValue());
                    continue;
                }
                FlagFromTagDimension dim = (FlagFromTagDimension) entry.getKey();

                Set<ApiFilter> transformedFilters = entry.getValue().stream()
                        .map((ApiFilter filter) -> new ApiFilter(
                                        dim.getFilteringDimension(),
                                        filter.getDimensionField(),
                                        transformFilterOperation(
                                                dim,
                                                filter.getOperation(),
                                                filter.getValues()
                                        ),
                                        Collections.singleton(dim.getTagValue())
                                )
                        )
                        .collect(Collectors.toSet());
                newFilters.put(dim.getFilteringDimension(), transformedFilters);
            }
            return request.withFilters(newFilters);
        }
    }
}
