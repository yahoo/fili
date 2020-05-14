package com.yahoo.bard.webservice.web.apirequest.generator.filter.antlr;

import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterDefinition;
import com.yahoo.bard.webservice.web.filters.FiltersBaseListener;
import com.yahoo.bard.webservice.web.filters.FiltersParser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO more full documentation
 * notes:
 *  * only good for single run, DON'T reuse listeners
 *  * getFilters() called after using to extract the constructed filters
 *  * not multithreaded safe and probably should not be manually interacted with in the middle of a tree walk
 */
public class FiltersListener extends FiltersBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(FiltersListener.class);

    private List<FilterDefinition> allFilterDefinitions;
    private FilterDefinition inProgressFilterDefinition;

    public FiltersListener() {
        this.allFilterDefinitions = new ArrayList<>();
    }

    /**
     * Gets the filters constructed by parsing an api filter clause. This getter should only be run AFTER the using
     * the listener to parse filters.
     *
     * @return
     */
    public List<FilterDefinition> getFilterDefinitions() {
        return new ArrayList<>(allFilterDefinitions);
    }

    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void enterFilter(FiltersParser.FilterContext ctx) {
        inProgressFilterDefinition = new FilterDefinition();
    }

    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void exitFilter(FiltersParser.FilterContext ctx) {
        allFilterDefinitions.add(new FilterDefinition(inProgressFilterDefinition));
    }

    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void exitDimension(FiltersParser.DimensionContext ctx) {
        inProgressFilterDefinition.setDimensionName(ctx.ID().getText());
    }

    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void exitField(FiltersParser.FieldContext ctx) {
        inProgressFilterDefinition.setFieldName(ctx.ID().getText());
    }
    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void exitOp(FiltersParser.OpContext ctx) {
        inProgressFilterDefinition.setOperationName(ctx.ID().getText());
    }

    /**
     * {@inheritDoc}
     *
     * TODO write documentation
     */
    @Override
    public void exitFilter_values(FiltersParser.Filter_valuesContext ctx) {
        inProgressFilterDefinition.setValues(
                ctx.VALUE().stream()
                    .map(TerminalNode::getText)
                    .collect(Collectors.toList())
        );
    }
}
