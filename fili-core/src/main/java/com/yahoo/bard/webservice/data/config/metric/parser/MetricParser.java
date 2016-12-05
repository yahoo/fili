// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser;

import com.yahoo.bard.webservice.data.config.metric.parser.lexer.LexException;
import com.yahoo.bard.webservice.data.config.metric.parser.lexer.Lexeme;
import com.yahoo.bard.webservice.data.config.metric.parser.lexer.LexemeType;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.ConstantMetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.IdentifierNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.MetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.ArithmeticOperator;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.BinaryFilterOperator;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.FilterOperator;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.FunctionOperator;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.Operator;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.Precedence;
import com.yahoo.bard.webservice.data.config.metric.parser.operator.Sentinel;
import com.yahoo.bard.webservice.data.config.provider.MakerDictionary;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Metric parser.
 */
public class MetricParser {

    private String name;

    // All lookups happen here; this is where the metrics all end up
    private final MetricDictionary dict;
    private final MetricDictionary tempDict;

    // Yes, this will eventually roll over. No, this is certainly not the right way to do this.
    private static long TEMP_IDENTIFIER = 0;
    private static String TEMP_BASE = "__temp_metric_";

    private final Deque<Operator> operatorStack = new ArrayDeque<>();
    private final Deque<Operand> operandStack = new ArrayDeque<>();

    private final DimensionDictionary dimensionDictionary;
    private final MakerDictionary makerDict;
    private final String metricDefinition;

    private Queue<Lexeme> items;

    /**
     * Parse a metric.
     *
     * @param metricName The metric name
     * @param metricDefinition The metric definition, as a string
     * @param dict the base metric dictionary
     * @param tempDict the temporary scoped metric dictionary
     * @param makerDict the metric maker dictionary
     * @param dimensionDictionary the dimension dictionary
     */
    public MetricParser(
            String metricName,
            String metricDefinition,
            MetricDictionary dict,
            MetricDictionary tempDict,
            MakerDictionary makerDict,
            DimensionDictionary dimensionDictionary
    ) {
        this.name = metricName;
        this.dict = dict;
        this.tempDict = tempDict;
        this.makerDict = makerDict;
        this.dimensionDictionary = dimensionDictionary;
        this.metricDefinition = metricDefinition;
    }

    /**
     * Parses the metric def and returns a LogicalMetric; caller should add to dictionary.
     *
     * @return a logical metric
     * @throws ParsingException when a lexing or parsing error occurs
     */
    public LogicalMetric parse() throws ParsingException {

        try {
            items = Lexeme.lex(metricDefinition);
        } catch (LexException e) {
            throw new ParsingException(
                    "Error occurred while lexing metric " + name + " with definition " + metricDefinition,
                    e
            );
        }

        try {
            MetricNode node = innerParse();
            return node.make(name, tempDict);
        } catch (ParsingException e) {
            throw new ParsingException(
                    "Error occurred while parsing metric " + name + " with definition " + metricDefinition,
                    e
            );
        }
    }

    /**
     * Parses the metric def and returns a MetricNode.
     *
     * @return a Metric Node
     * @throws ParsingException when a parsing error occurs
     */
    protected MetricNode innerParse() throws ParsingException {
        operatorStack.push(new Sentinel());
        parseExpression();
        Operand o = operandStack.peek();
        return o.getMetricNode();
    }

    /**
     * Parse an expression
     *
     * Uses the shunting yard algorithm ( https://www.engr.mun.ca/~theo/Misc/exp_parsing.htm#shunting_yard or wiki),
     * with some minor changes to handle things like function calls.
     *
     * @throws ParsingException when a parsing error occurs
     */
    public void parseExpression() throws ParsingException {
        consumeNext();

        // Handle most operators; functions are handled inside consumeNext()
        while (true) {
            Lexeme current;
            if ((current = peekPop(LexemeType.BINARY_OPERATOR)) != null) {
                pushOperator(ArithmeticOperator.fromString(current.getToken()));
                consumeNext();
            } else if ((current = peekPop(LexemeType.FILTER_OPERATOR)) != null) {
                pushOperator(BinaryFilterOperator.fromString(current.getToken()));
                consumeNext();
            } else if ((peekPop(LexemeType.PIPE)) != null) {
                pushOperator(new FilterOperator());
                consumeNext();
            } else {
                break;
            }
        }

        while (operatorStack.peek().getPrecedence().greaterThan(Precedence.SENTINEL)) {
            popOperator();
        }
    }

    /**
     * Consume and parse the next token/lexeme on the stack.
     *
     * @throws ParsingException when a parsing error occurs
     */
    private void consumeNext() throws ParsingException {
        Lexeme current = items.remove();
        LexemeType type = current.getType();

        switch (type) {
            case NUMBER:
            case DOUBLE_QUOTED_STRING:
            case SINGLE_QUOTED_STRING:
                operandStack.push(new ConstantMetricNode(current.getToken()));
                break;
            case L_PAREN:
                operatorStack.push(new Sentinel());
                parseExpression();
                expect(LexemeType.R_PAREN);
                operatorStack.pop();
                break;

            case IDENTIFIER:
                if (peekType(LexemeType.L_PAREN)) {
                    expect(LexemeType.L_PAREN);
                    int nArgs = parseFuncArgs();
                    expect(LexemeType.R_PAREN);
                    pushOperator(new FunctionOperator(makerDict.get(current.getToken()), nArgs));
                } else {
                    operandStack.push(new IdentifierNode(current.getToken(), dimensionDictionary, dict));
                }
                break;
            default:
                throw new ParsingException("Unexpected token:" + current.getToken());
        }
    }

    /**
     * Helper function to parse arguments to a function.
     *
     * @return the number of args parsed and placed onto the operand stack
     * @throws ParsingException when a parsing error occurs
     */
    private int parseFuncArgs() throws ParsingException {
        int argc = 0;

        while (true) {
            operatorStack.push(new Sentinel());
            parseExpression();
            argc += 1;
            operatorStack.pop();

            if (!peekType(LexemeType.COMMA)) {
                break;
            }
            expect(LexemeType.COMMA);
        }

        return argc;
    }

    /**
     * Get the next item on the top of the lexeme stack if it matches `type`, or throw an exception.
     *
     * @param type Lexeme type
     * @return the item of the specified type
     * @throws ParsingException when a parsing error occurs
     */
    private Lexeme expect(LexemeType type) throws ParsingException {
        Lexeme current = items.remove();
        if (current.getType() != type) {
            throw new ParsingException("Unexpected token type: found " + current.getType() + " but expected " + type);
        }
        return current;
    }

    /**
     * Return true if the lexeme on the top of the stack equals the passed-in type.
     *
     * @param type lexeme type
     * @return true if the top item matches, false otherwise
     */
    private boolean peekType(LexemeType type) {
        return items.size() != 0 && (items.peek().getType() == type);
    }

    /**
     * Return the next lexeme if it matches `type`, or null.
     *
     * @param type lexeme type
     * @return item if the top item's type matches, null otherwise
     * @throws ParsingException when a parsing error occurs
     */
    private Lexeme peekPop(LexemeType type) throws ParsingException {
        if (peekType(type)) {
            return expect(type);
        }

        return null;
    }

    /**
     * Operator stack pop.
     *
     * @throws ParsingException when an error occurs building
     */
    private void popOperator() throws ParsingException {
        if (operatorStack.peek().getPrecedence().greaterThan(Precedence.SENTINEL)) {
            Operator operator = operatorStack.pop();
            LinkedList<Operand> operands = new LinkedList<>();

            for (int i = 0; i < operator.getNumOperands(); i++) {
                operands.addFirst(operandStack.pop());
            }

            operandStack.push(operator.build(operands));
        }
    }

    /**
     * Operator stack push.
     *
     * @param op the operator to add to the stack
     *
     * @throws ParsingException when an error occurs building
     */
    private void pushOperator(Operator op) throws ParsingException {
        while (true) {
            if (operatorStack.peek().greaterThan(op)) {
                popOperator();
            } else {
                break;
            }
        }
        operatorStack.push(op);
    }

    /**
     * Generate a globally unique new name.
     * <p>
     * Note: this assumes that we've chosen a sufficiently unique prefix.
     *
     * @return the new name
     */
    public synchronized static String getNewName() {
        return TEMP_BASE + TEMP_IDENTIFIER++;
    }
}
