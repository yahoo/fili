// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy.antlr;


import static com.yahoo.bard.webservice.web.ErrorMessageFormat.SORT_INVALID_WITH_DETAIL;

import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadOrderByException;
import com.yahoo.bard.webservice.web.sorts.SortsLex;
import com.yahoo.bard.webservice.web.sorts.SortsParser;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Parser that reads a string and uses ANTLR to extract logical representatons of sort columns.
 */
public class ProtocolAntlrSortParser {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolAntlrSortParser.class);

    /**
     * Transform a text string into a protocol metric compliant order by column.
     *
     * @param orderByQuery  The request parameter
     *
     * @return A list of order by columns (not bound to type yet)
     *
     * @throws BadApiRequestException if the string cannot be parsed.
     */
    public List<OrderByColumn> apply(
            String orderByQuery
    ) throws BadApiRequestException {
        LOG.trace("Sorts query: {}", orderByQuery);

        if (orderByQuery == null || "".equals(orderByQuery)) {
            return Collections.emptyList();
        }

        SortsListListener listener = new SortsListListener();
        SortsLex lexer = SortsGrammarUtils.getLexer(orderByQuery);
        SortsParser parser = SortsGrammarUtils.getParser(lexer);
        try {
            SortsParser.SortsContext tree = parser.sorts();
            ParseTreeWalker.DEFAULT.walk(listener, tree);
        } catch (ParseCancellationException parseException) {
            LOG.debug(SORT_INVALID_WITH_DETAIL.logFormat(orderByQuery, parseException.getMessage()));
            throw new BadOrderByException(
                    SORT_INVALID_WITH_DETAIL.format(orderByQuery, parseException.getMessage()),
                    parseException.getCause()
            );
        }
        return listener.getResults();
    }
}
