grammar Havings;

options {
    tokenVocab=HavingsLex;
}

havings
    : ( havingComponent ( COMMA havingComponent )* )? EOF;

havingComponent
    : metric DASH operator OPEN_BRACKET havingValues CLOSE_BRACKET;

metric
    : metricName ( OPEN_PARENTHESIS params? CLOSE_PARENTHESIS )?
    ;
metricName
    : ID
    ;

operator
    : ID
    ;
params
    : paramValue ( COMMA paramValue )*
    ;

paramValue
    : ID EQUALS (VALUE | ESCAPED_VALUE)
    ;

havingValues
    : HAVING_VALUE ( COMMA HAVING_VALUE )*
    ;
