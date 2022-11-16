grammar Metrics;

options {
    tokenVocab=MetricsLex;
}
metrics
    : ( metric ( COMMA metric )* )? EOF;

metric
    : metricName ( OPEN_PARENTHESIS params? CLOSE_PARENTHESIS )?
    ;
metricName
    : ID
    ;
params
    : paramValue ( COMMA paramValue )*
    ;
paramValue
    : ID EQUALS (VALUE | ESCAPED_VALUE)
    ;
