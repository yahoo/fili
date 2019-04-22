// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import static com.yahoo.bard.webservice.util.StreamUtils.not;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.QueryType;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.QueryContext;
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Template Druid Query. This class is immutable.
 */
public class TemplateDruidQuery implements DruidAggregationQuery<TemplateDruidQuery> {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateDruidQuery.class);

    private final TemplateDruidQuery nestedQuery;
    private final ZonelessTimeGrain timeGrain;
    private final Set<Aggregation> aggregations;
    private final Set<PostAggregation> postAggregations;
    private final int depth;

    /**
     * Template Query constructor for a non nested template query. For a non nested query (i.e. last node in the link),
     * it does not have any nested query
     *
     * @param aggregations  aggregations for this query template
     * @param postAggregations  post aggregations for this query template
     */
    public TemplateDruidQuery(Collection<Aggregation> aggregations, Collection<PostAggregation> postAggregations) {
        this(aggregations, postAggregations, null, (ZonelessTimeGrain) null);
    }

    /**
     * Template Query constructor for a non nested template query with a bound time grain.
     *
     * @param aggregations  aggregations for this query template
     * @param postAggregations  post aggregations for this query template
     * @param timeGrain  The time grain constraint
     */
    public TemplateDruidQuery(
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            ZonelessTimeGrain timeGrain
    ) {
        this(aggregations, postAggregations, null, timeGrain);
    }

    /**
     * Template Query constructor for a nested template query.
     *
     * @param aggregations  aggregations for this query template
     * @param postAggregations  post aggregations for this template query
     * @param nestedQuery  A query which this query uses as a data source
     */
    public TemplateDruidQuery(
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            TemplateDruidQuery nestedQuery
    ) {
        this(aggregations, postAggregations, nestedQuery, (ZonelessTimeGrain) null);
    }

    /**
     * Template Query constructor for a nested template query with a bound time grain.
     *
     * @param aggregations  aggregations for this query template
     * @param postAggregations  post aggregations for this query template
     * @param nestedQuery  A query which this query uses as a data source
     * @param timeGrain  The time grain constraint on the query if any
     */
    public TemplateDruidQuery(
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations,
            TemplateDruidQuery nestedQuery,
            ZonelessTimeGrain timeGrain
    ) {
        // Convert the sets to LinkedHashSet to preserve order, and then make them unmodifiable
        this.aggregations = Collections.unmodifiableSet(new LinkedHashSet<>(aggregations));
        this.postAggregations = Collections.unmodifiableSet(new LinkedHashSet<>(postAggregations));
        this.nestedQuery = nestedQuery;
        this.timeGrain = timeGrain;

        // Check for duplicate field names
        Set<String> nameCollisions = getNameCollisions(aggregations, postAggregations);
        if (!nameCollisions.isEmpty()) {
            String message = "Duplicate name in aggregation & post aggregations: " + nameCollisions;
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        depth = calculateDepth(this);
    }

    /**
     * Gather duplicate names across the collection of Aggregations and PostAggregations.
     *
     * @param aggregations  Set of Aggregations to inspect
     * @param postAggregations  Set of PostAggregations to inspect
     *
     * @return Set of collided names (if any)
     */
    private Set<String> getNameCollisions(
            Collection<Aggregation> aggregations,
            Collection<PostAggregation> postAggregations
    ) {
        Set<String> allNames = new HashSet<>();
        return Stream.concat(aggregations.stream(), postAggregations.stream())
                .map(MetricField::getName)
                .filter(not(allNames::add)) // Select names that already had been added to allNames
                .collect(Collectors.toSet());
    }

    /**
     * Transforms a N-pass query into a (N+1)-pass query. The original query is not mutated.
     *
     * @return nested query
     */
    public TemplateDruidQuery nest() {

        /*
         * each aggregation needs to be split into an inner & outer. Sometimes this involves transformation of the
         * aggregation type, name, or field name.
         */
        LinkedHashSet<Aggregation> innerAggregations = new LinkedHashSet<>();
        LinkedHashSet<Aggregation> outerAggregations = new LinkedHashSet<>();
        for (Aggregation agg : aggregations) {
            Pair<Optional<Aggregation>, Optional<Aggregation>> split = agg.nest();
            split.getRight().ifPresent(innerAggregations::add);
            split.getLeft().ifPresent(outerAggregations::add);
        }

        // Create the inner query.
        TemplateDruidQuery innerQuery;
        if (isNested()) {
            innerQuery = new TemplateDruidQuery(innerAggregations, Collections.emptySet(), nestedQuery, null);
        } else {
            innerQuery = new TemplateDruidQuery(
                    innerAggregations,
                    Collections.emptySet(),
                    null,
                    null
            );
        }

        // Create the outer query, floating the post aggregations upward
        return new TemplateDruidQuery(outerAggregations, postAggregations, innerQuery, timeGrain);
    }

    /**
     * Check if outer TimeGrain is compatible with inner TimeGrain.
     *
     * @return false if outer TimeGrain cannot be composed by the inner time grain
     */
    public boolean isTimeGrainValid() {
        if (nestedQuery != null) {
            TimeGrain nestedTimeGrain = nestedQuery.getTimeGrain();
            // Nested time grain must be smaller or equal to this time grain
            return timeGrain == null || nestedTimeGrain == null || timeGrain.satisfiedBy(nestedTimeGrain);
        }

        return true;
    }

    /**
     * Merges two template queries into one. The original queries are not mutated.
     *
     * @param sibling  the query to merge.
     *
     * @return merged query
     */
    public TemplateDruidQuery merge(TemplateDruidQuery sibling) {

        // TODO: Handle merging with a null TDQ

        // Correct the queries to have the same depth by nesting if necessary.
        TemplateDruidQuery self = this;
        while (self.depth > sibling.depth) {
            sibling = sibling.nest();
        }
        while (sibling.depth > self.depth) {
            self = self.nest();
        }

        // Merge together all the aggregations and post aggregations for the outer query.
        Set<Aggregation> mergedAggregations = mergeAggregations(self.getAggregations(), sibling.getAggregations());
        LinkedHashSet<PostAggregation> mergedPostAggregations = new LinkedHashSet<>(self.getPostAggregations());
        mergedPostAggregations.addAll(sibling.getPostAggregations());

        // Merge the time grains
        ZonelessTimeGrain mergedGrain = mergeTimeGrains(self.getTimeGrain(), sibling.getTimeGrain());
        TemplateDruidQuery mergedNested = self.isNested() ?
                self.nestedQuery.merge(sibling.getInnerQuery().get())
                : null;
        return new TemplateDruidQuery(
                mergedAggregations,
                mergedPostAggregations,
                mergedNested,
                mergedGrain
        );
    }

    /**
     * Given two sets of Aggregations, merge them into a single set of Aggregations, combining where possible.
     *
     * @param set1  First set of Aggregations
     * @param set2  Second set of Aggregations
     *
     * @return the merged Aggregations
     */
    private Set<Aggregation> mergeAggregations(Set<Aggregation> set1, Set<Aggregation> set2) {

        // Index the 1st set of aggregations by name. This value set is also our result set
        Map<String, Aggregation> resultAggregationsByName = new LinkedHashMap<>();
        for (Aggregation agg : set1) {
            // Put and check for overwriting an existing name, indicating that we had 2 aggregations with the same name
            if (resultAggregationsByName.put(agg.getName(), agg) != null) {
                String message = String.format("Duplicate name %s in aggregation set %s", agg.getName(), set1);
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
        }

        // Walk the other aggregations and add them to the result set if they are missing, making conversions as needed
        for (Aggregation thatOne : set2) {
            // See if we have an aggregation already with the same name
            Aggregation thisOne = resultAggregationsByName.get(thatOne.getName());

            // Add this aggregation to the result set if there isn't an agg with the same name, or it's an exact mach
            if (thisOne == null || thisOne.equals(thatOne)) {
                resultAggregationsByName.put(thatOne.getName(), thatOne);
                continue;
            }

            // If a sketch and a sketch collide, and one is a merge, then replace both by a merge on the name/fieldName
            // TODO: Need more clarity on what this is actually for
            if (thisOne.isSketch() && thatOne.isSketch() && thisOne.getFieldName().equals(thatOne.getFieldName())) {
                SketchAggregation converted = FieldConverterSupplier
                        .getSketchConverter()
                        .asInnerSketch((SketchAggregation) thisOne);
                resultAggregationsByName.remove(thisOne.getName());
                resultAggregationsByName.put(converted.getName(), converted);
                continue;
            }

            // We can't handle merging this aggregation
            String message = "Attempt to merge sketch aggregations with the same name, but over different field names";
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }

        return new LinkedHashSet<>(resultAggregationsByName.values());
    }

    /**
     * Merge two time grains together.
     * <p/>
     * This is the pattern for how the time grains are merged:
     * <ul>
     *     <li>null - null = null</li>
     *     <li>nonNull - null = nonNull</li>
     *     <li>nonNull - nonNull = nonNull</li>
     *     <li>nonNull - differentNonNull = ERROR</li>
     * </ul>
     *
     * @param timeGrain1  First time grain to merge
     * @param timeGrain2  Second time grain to merge
     *
     * @return The merged time grain
     */
    private ZonelessTimeGrain mergeTimeGrains(ZonelessTimeGrain timeGrain1, ZonelessTimeGrain timeGrain2) {
        if (timeGrain1 == null) {
            return timeGrain2;
        } else if (timeGrain2 == null || timeGrain1.equals(timeGrain2)) {
            return timeGrain1;
        } else {
            String message = String.format("Cannot merge mismatched time grains %s and %s", timeGrain1, timeGrain2);
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public String toString() {
        return "TemplateDruidQuery{\n" +
                "druidAggregations=" + aggregations + ",\n" +
                "postAggregations=" + postAggregations + ",\n" +
                "nestedQuery=" + nestedQuery + ",\n" +
                "timeGrain=" + timeGrain + "\n" +
                "}";
    }

    @Override
    public QueryType getQueryType() {
        return null;
    }

    @Override
    public DataSource getDataSource() {
        return null;
    }

    @Override
    public QueryContext getContext() {
        return null;
    }

    @Override
    public Granularity getGranularity() {
        return timeGrain;
    }

    @Override
    public Filter getFilter() {
        return null;
    }

    @Override
    public List<Interval> getIntervals() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Dimension> getDimensions() {
        return Collections.emptySet();
    }

    @Override
    public Set<Aggregation> getAggregations() {
        return aggregations;
    }

    @Override
    public Set<PostAggregation> getPostAggregations() {
        return postAggregations;
    }

    public ZonelessTimeGrain getTimeGrain() {
        return timeGrain;
    }

    @Override
    public Optional<TemplateDruidQuery> getInnerQuery() {
        return Optional.ofNullable(nestedQuery);
    }

    @Override
    public TemplateDruidQuery getInnermostQuery() {
        return (TemplateDruidQuery) DruidAggregationQuery.super.getInnermostQuery();
    }

    /**
     * Checks if the template druid query is nested.
     *
     * @return true if query is nested else false.
     */
    public boolean isNested() {
        return (depth() > 1);
    }

    /**
     * Returns the depth of the query nesting.
     *
     * @return 1 for queries without nesting. &gt;1 for queries with nested queries.
     */
    public int depth() {
        return depth;
    }

    /**
     * Calculate the depth of the candidate TemplateDruidQuery.
     *
     * @param candidate  TemplateDruidQuery to calculate the depth of
     *
     * @return The depth of the candidate query
     */
    private int calculateDepth(TemplateDruidQuery candidate) {
        int theDepth = 1;
        Optional<TemplateDruidQuery> iterator = candidate.getInnerQuery();
        while (iterator.isPresent()) {
            theDepth++;
            iterator = iterator.get().getInnerQuery();
        }
        return theDepth;
    }

    /**
     * Get the field by name.
     *
     * @param name  Name of the field to retrieve
     *
     * @return The field if found, or null if we couldn't find a matching Field
     * @throws IllegalArgumentException if there is no MetricField with the given name
     */
    public MetricField getMetricField(String name) {
        return Stream.concat(postAggregations.stream(), aggregations.stream())
                .filter(field -> field.getName().equals(name))
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }

    /**
     * Makes a copy of the template query and any sub query(s), changing aggregations.
     * <p>
     * Everything is a shallow copy.
     *
     * @param newAggregations  The Aggregations to replace in the copy
     *
     * @return copy of the query
     */
    @Override
    public TemplateDruidQuery withAggregations(Collection<Aggregation> newAggregations) {
        return new TemplateDruidQuery(newAggregations, postAggregations, nestedQuery, timeGrain);
    }

    /**
     * Makes a copy of the template query and any sub query(s), changing post-aggregations.
     * <p>
     * Everything is a shallow copy.
     *
     * @param newPostAggregations  The PostAggregations to replace with in the copy
     *
     * @return copy of the query
     */
    public TemplateDruidQuery withPostAggregations(Collection<PostAggregation> newPostAggregations) {
        return new TemplateDruidQuery(aggregations, newPostAggregations, nestedQuery, timeGrain);
    }

    /**
     * Makes a copy of the template query, changing nested query.
     * <p>
     * Everything is a shallow copy.
     *
     * @param newNestedQuery  The nestedQuery to replace in the copy
     *
     * @return copy of the query
     */
    public TemplateDruidQuery withInnerQuery(TemplateDruidQuery newNestedQuery) {
        return new TemplateDruidQuery(aggregations, postAggregations, newNestedQuery, timeGrain);
    }

    /**
     * Makes a copy of the template query and any sub query(s), changing time grain on the outermost level only.
     * <p>
     * Everything is a shallow copy.
     *
     * @param newTimeGrain  The TimeGrain to replace with in the copy
     *
     * @return copy of the query
     */
    public TemplateDruidQuery withGranularity(ZonelessTimeGrain newTimeGrain) {
        return new TemplateDruidQuery(aggregations, postAggregations, nestedQuery, newTimeGrain);
    }

    @Override
    public TemplateDruidQuery withDataSource(DataSource dataSource) {
        return this;
    }

    @Override
    public TemplateDruidQuery withInnermostDataSource(DataSource dataSource) {
        return this;
    }

    /**
     * With granularity is partially implemented because TemplateDruidQuery supports only ZonelessTimeGrains.
     *
     * @param granularity  a zoneless time grain
     *
     * @return a new TemplateDruidQuery based in the new granularity
     * @throws UnsupportedOperationException if the granularity is not a ZonelessTimeGrain
     */
    @Override
    public TemplateDruidQuery withGranularity(Granularity granularity) {
        if (granularity instanceof ZonelessTimeGrain) {
            return withGranularity((ZonelessTimeGrain) granularity);
        }
        throw new UnsupportedOperationException("Template Druid Query only supports Zoneless Time Grains");
    }

    @Override
    public TemplateDruidQuery withFilter(Filter filter) {
        return this;
    }


    @Override
    public TemplateDruidQuery withIntervals(Collection<Interval> intervals) {
        return this;
    }

    @Override
    public TemplateDruidQuery withAllIntervals(Collection<Interval> intervals) {
        return this;
    }

    @Override
    public TemplateDruidQuery withContext(QueryContext context) {
        return this;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) { return true; }
        if (!(o instanceof TemplateDruidQuery)) { return false; }

        TemplateDruidQuery that = (TemplateDruidQuery) o;

        return
                Objects.equals(aggregations, that.aggregations) &&
                        Objects.equals(postAggregations, that.postAggregations) &&
                        Objects.equals(nestedQuery, that.nestedQuery) &&
                        Objects.equals(timeGrain, that.timeGrain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations, postAggregations, nestedQuery, timeGrain);
    }
}
