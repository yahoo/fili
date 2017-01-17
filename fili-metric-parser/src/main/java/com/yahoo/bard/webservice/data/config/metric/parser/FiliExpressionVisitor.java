package com.yahoo.bard.webservice.data.config.metric.parser;

import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricBaseVisitor;
import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricParser;
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.FilteredAggregationMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Antlr visitor for filiExpression grammar rule.
 */
public class FiliExpressionVisitor extends FiliMetricBaseVisitor<LogicalMetric> {

    // This is obnoxious. I'm not sure whether
    // 1) This needs to be static (do these need to be globally unique?)
    // 2) This needs to exist (is there a better way?)
    private static long TEMP_IDENTIFIER = 0;
    private static String TEMP_BASE = "__temp_metric_";

    protected final ParseContext context;

    /**
     * Construct a new visitor for the 'filiExpression' rule.
     *
     * Delegates filter expressions to their own visitor.
     *
     * @param context parse context
     */
    public FiliExpressionVisitor(ParseContext context) {
        this.context = context;
    }

    @Override
    public LogicalMetric visitFiliExpression(FiliMetricParser.FiliExpressionContext ctx) {
        LogicalMetric metric = visitExpression(ctx.expression());

        if (ctx.filterExp() != null) {
            FiliFilterVisitor filterVisitor = new FiliFilterVisitor(context);
            Filter filter = filterVisitor.visit(ctx.filterExp());

            // I don't think we can have more than one aggregation here.
            Aggregation aggregation = metric
                    .getTemplateDruidQuery()
                    .getAggregations()
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "Filtered aggregation requires aggregation but could not find one"));

            FilteredAggregationMaker maker = new FilteredAggregationMaker(context.scopedDict, aggregation, filter);

            // FIXME: I don't think dependant metrics are needed as we're wrapping the other metric (not sure).
            metric = maker.make(getNewName(), Collections.emptyList());
        }

        // FIXME: this is kind of gross
        LogicalMetric renamedMetric = new LogicalMetric(
                metric.getTemplateDruidQuery(),
                metric.getCalculation(),
                context.name,
                metric.getLongName(),
                metric.getCategory(),
                metric.getDescription()
        );

        return renamedMetric;
    }

    /**
     * Given left and right sides and antlr operator type, build an arithmetic post agg.
     *
     * @param lhs left side parameter
     * @param rhs right side parameter
     * @param type antlr4 token type id
     *
     * @return arithmetic metric node
     */
    public static LogicalMetric buildArithmeticNode(ParseContext ctx, LogicalMetric lhs, LogicalMetric rhs, int type) {
        ArithmeticPostAggregation.ArithmeticPostAggregationFunction function;

        switch (type) {
            case FiliMetricParser.PLUS:
                function = ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS;
                break;
            case FiliMetricParser.MINUS:
                function = ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MINUS;
                break;
            case FiliMetricParser.MUL:
                function = ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MULTIPLY;
                break;
            case FiliMetricParser.DIV:
                function = ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE;
                break;
            default:
                throw new RuntimeException("Unknown type: " + type);
        }

        MetricMaker maker = new ArithmeticMaker(ctx.scopedDict, function, new NoOpResultSetMapper());
        LogicalMetric result = maker.make(getNewName(), Arrays.asList(lhs.getName(), rhs.getName()));
        ctx.scopedDict.add(result);
        return result;

    }

    @Override
    public LogicalMetric visitPlusMinusExpression(final FiliMetricParser.PlusMinusExpressionContext ctx) {
        LogicalMetric lhs;
        LogicalMetric rhs;

        if (ctx.plusMinusExpression() != null) {
            lhs = visitPlusMinusExpression(ctx.plusMinusExpression());
            rhs = visitPlusMinusArg(ctx.plusMinusArg(0));
        } else if (ctx.plusMinusArg(0) != null) {
            lhs = visitPlusMinusArg(ctx.plusMinusArg(0));
            rhs = visitPlusMinusArg(ctx.plusMinusArg(1));
        } else {
            throw new RuntimeException("error: could not parse");
        }

        // This returns a logical metric that has been added to the
        // scopedDict with a new temporary name.
        return buildArithmeticNode(context, lhs, rhs, ctx.operator.getType());
    }

    @Override
    public LogicalMetric visitMulDivExpression(final FiliMetricParser.MulDivExpressionContext ctx) {
        LogicalMetric lhs;
        LogicalMetric rhs;

        if (ctx.mulDivExpression() != null) {
            lhs = visitMulDivExpression(ctx.mulDivExpression());
            rhs = visitAtom(ctx.atom(0));
        } else {
            lhs = visitAtom(ctx.atom(0));
            rhs = visitAtom(ctx.atom(1));
        }

        // This returns a logical metric that has been added to the
        // scopedDict with a new temporary name.
        return buildArithmeticNode(context, lhs, rhs, ctx.operator.getType());
    }

    @Override
    public LogicalMetric visitAnynum(final FiliMetricParser.AnynumContext ctx) {
        String value;
        if (ctx.DECIMAL() != null) {
            value = ctx.DECIMAL().getText();
        } else if (ctx.INTEGER() != null) {
            value = ctx.INTEGER().getText();
        } else {
            throw new RuntimeException("Error");
        }

        ConstantMaker maker = new ConstantMaker(context.scopedDict);
        LogicalMetric metric = maker.make(getNewName(), Collections.singletonList(value));
        context.scopedDict.add(metric);
        return metric;
    }

    @Override
    public LogicalMetric visitFunction(final FiliMetricParser.FunctionContext ctx) {
        String functionName = ctx.IDENTIFIER().getText();
        List<LogicalMetric> operands = new ArrayList<>();

        if (ctx.param_list() != null && ctx.param_list().expression() != null) {
            List<FiliMetricParser.ExpressionContext> children = ctx.param_list().expression();
            for (FiliMetricParser.ExpressionContext child : children) {
                operands.add(visitExpression(child));
            }
        }


        List<String> parameterNames = operands
                .stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toList());

        MetricMaker maker = context.makerBuilder.build(functionName, context.scopedDict);
        LogicalMetric metric = maker.make(getNewName(), parameterNames);
        context.scopedDict.add(metric);
        return metric;
    }

    @Override
    public LogicalMetric visitAtom(final FiliMetricParser.AtomContext ctx) {
        if (ctx.anynum() != null) {
            return visitAnynum(ctx.anynum());
        } else if (ctx.expression() != null) {
            return visitExpression(ctx.expression());
        } else if (ctx.function() != null) {
            return visitFunction(ctx.function());
        } else if (ctx.IDENTIFIER() != null) {

            // If we come across an identifier, it could be referring to either an existing (dependant) metric,
            // or a raw metric. This seems like a hack, but:
            //
            // - If we can find an existing logical metric in the dictionary, use that
            // - Otherwise, return a new (empty) logical metric, with only a name. Presumably this is a raw name.
            //   Unlike every other kind of metric produced by this parser, raw names are currently not added
            //   to the scopedDict. Not sure if this is correct or not...
            // FIXME added because it seems like there should be a better way to do this.
            String name = ctx.IDENTIFIER().getText();
            if (context.scopedDict.containsKey(name)) {
                return context.scopedDict.get(name);
            } else {
                return new LogicalMetric(null, null, name);
            }
        } else {
            throw new RuntimeException("Unknown atom");
        }
    }

    /**
     * Generate a globally unique new name.
     * <p>
     * Note: this assumes that we've chosen a sufficiently unique prefix.
     *
     * It's not clear to me at this time whether this needs to be globally unique or if uniqueness per-metric
     * is OK.
     *
     * @return the new name
     */
    private synchronized static String getNewName() {
        return TEMP_BASE + TEMP_IDENTIFIER++;
    }

}
