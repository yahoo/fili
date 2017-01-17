package com.yahoo.bard.webservice.data.config.metric.parser;

import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricBaseVisitor;
import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricParser;
import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;

import java.util.Arrays;

/**
 * Visitor for antlr4 rule 'filterExp'.
 */
public class FiliFilterVisitor extends FiliMetricBaseVisitor<Filter> {
    protected final ParseContext context;

    /**
     * Construct a new visitor for 'filterExp'.
     *
     * @param context parse context
     */
    public FiliFilterVisitor(ParseContext context) {
        this.context = context;
    }

    @Override
    public Filter visitAndOrExp(final FiliMetricParser.AndOrExpContext ctx) {
        Filter lhs, rhs;
        if (ctx.andOrExp() != null) {
            lhs = visitAndOrExp(ctx.andOrExp());
            rhs = visitAndOrArg(ctx.andOrArg(0));
        } else {
            lhs = visitAndOrArg(ctx.andOrArg(0));
            rhs = visitAndOrArg(ctx.andOrArg(1));
        }


        if (ctx.operator.getType() == FiliMetricParser.AND) {
            return new AndFilter(Arrays.asList(lhs, rhs));
        } else if (ctx.operator.getType() == FiliMetricParser.OR) {
            return new OrFilter(Arrays.asList(lhs, rhs));
        } else {
            throw new RuntimeException("Could not parse filter");
        }
    }

    @Override
    public Filter visitEqualExp(final FiliMetricParser.EqualExpContext ctx) {
        String lhs, rhs;
        lhs = ctx.IDENTIFIER(0).getText();
        if (ctx.anynum() != null) {
            rhs = ctx.anynum().getText();
        } else if(ctx.IDENTIFIER(1) != null) {
            rhs = ctx.IDENTIFIER(1).getText();
        } else {
            throw new RuntimeException("Unknown parameter");
        }

        return new SelectorFilter(context.dimensionDictionary.findByApiName(lhs), rhs);
    }
}
