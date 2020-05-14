package com.yahoo.bard.webservice.web.apirequest.generator.filter.antlr;

import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadFilterException;
import com.yahoo.bard.webservice.web.apirequest.generator.filter.ApiFilterParser;
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterDefinition;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParser;
import com.yahoo.bard.webservice.web.filters.FiltersLex;
import com.yahoo.bard.webservice.web.filters.FiltersParser;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import javax.validation.constraints.NotNull;

/**
 * Parses string filter queries using an Antlr based grammar.
 */
public class AntlrApiFilterParser implements ApiFilterParser {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolAntlrApiMetricParser.class);

    @Override
    public FilterDefinition parseSingleApiFilterQuery(@NotNull String singleFilter) throws BadFilterException {
        java.util.Objects.requireNonNull(singleFilter);
        if (singleFilter.isEmpty()) {
            throw new IllegalArgumentException("When parsing single filter, filter must NOT be empty or null");
        }
        return parseApiFilterQuery(singleFilter).get(0);
    }

    @Override
    public List<FilterDefinition> parseApiFilterQuery(String filterQuery) throws BadFilterException {
        if (filterQuery == null || "".equals(filterQuery)) {
            return Collections.emptyList();
        }

        FiltersListener listener = new FiltersListener();
        FiltersLex lexer = FilterGrammarUtils.getLexer(filterQuery);
        FiltersParser parser = FilterGrammarUtils.getParser(lexer);
        try {
            FiltersParser.FiltersContext tree = parser.filters();
            ParseTreeWalker.DEFAULT.walk(listener, tree);
        } catch (ParseCancellationException parseException) {

            String message = String.format(
                    "ERROR: unable to generate api filters from filter query %s",
                    filterQuery
            );
            LOG.debug(message);
            throw new BadFilterException(message);
        }

        // split assign and return for debugging convenience
        List<FilterDefinition> generated = listener.getFilterDefinitions();
        return generated;
    }
}
