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

    public static final Set<FilterOperation> DEFAULT_POSITIVE_OPS = Stream.of(
            DefaultFilterOperation.in,
            DefaultFilterOperation.startswith,
            DefaultFilterOperation.contains,
            DefaultFilterOperation.eq
    ).collect(Collectors.toSet());

    public static final Set<FilterOperation> DEFAULT_NEGATIVE_OPS = Collections.singleton(
            DefaultFilterOperation.notin
    );

    protected Set<FilterOperation> positiveOps;
    protected Set<FilterOperation> negativeOps;

    public FlagToTagApiFilterTransformRequestMapperProvider() {
        this(DEFAULT_POSITIVE_OPS, DEFAULT_NEGATIVE_OPS);
    }

    public FlagToTagApiFilterTransformRequestMapperProvider(
            Set<FilterOperation> positiveOps,
            Set<FilterOperation> negativeOps
    ) {
        this.positiveOps = positiveOps;
        this.negativeOps = negativeOps;
    }

    public RequestMapper<DataApiRequest> dataApiRequestMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<DataApiRequest>(dictionaries) {
            @Override
            public DataApiRequest apply(DataApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformApiFilters(request.getApiFilters()));
            }
        };
    }

    public RequestMapper<DimensionsApiRequest> dimensionsApiRequestMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<DimensionsApiRequest>(dictionaries) {
            @Override
            public DimensionsApiRequest apply(DimensionsApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformSetOfApiFilters(request.getFilters()));
            }
        };
    }

    public RequestMapper<TablesApiRequest> tablesApiRequestMapper(ResourceDictionaries dictionaries) {
        return new RequestMapper<TablesApiRequest>(dictionaries) {
            @Override
            public TablesApiRequest apply(TablesApiRequest request, ContainerRequestContext context)
                    throws RequestValidationException {
                return request.withFilters(transformApiFilters(request.getApiFilters()));
            }
        };
    }

    protected Set<ApiFilter> transformSetOfApiFilters(Set<ApiFilter> apiFilters) {
        Set<ApiFilter> newFilters = new HashSet<>();
        for (ApiFilter filter : apiFilters) {
            ApiFilter newFilter = filter;
            if (filter.getDimension() instanceof FlagFromTagDimension) {
                FlagFromTagDimension dim = (FlagFromTagDimension) filter.getDimension();
                newFilter = new ApiFilter(
                        dim.getFilteringDimension(),
                        filter.getDimensionField(),
                        transformFilterOperation(
                                dim,
                                filter.getOperation(),
                                filter.getValues()
                        ),
                        Collections.singleton(dim.getTagValue())
                );
            }
            newFilters.add(newFilter);
        }
        return newFilters;
    }

    protected ApiFilters transformApiFilters(ApiFilters requestFilters) {
        ApiFilters newFilters = new ApiFilters();
        for (Map.Entry<Dimension, Set<ApiFilter>> entry : requestFilters.entrySet()) {
            if (!(entry.getKey() instanceof FlagFromTagDimension)) {
                newFilters.put(entry.getKey(), entry.getValue());
                continue;
            }
            FlagFromTagDimension dim = (FlagFromTagDimension) entry.getKey();
            Set<ApiFilter> transformedFilters = entry.getValue().stream()
                    .map((ApiFilter filter) -> new ApiFilter(
                                    dim.getFilteringDimension(),
                                    dim.getFilteringDimension().getKey(),
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
        return newFilters;
    }

    protected FilterOperation transformFilterOperation(
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
}
