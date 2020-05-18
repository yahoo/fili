grammar Sorts;

options {
    tokenVocab=SortsLex;
}

sorts
    : ( sortsComponent ( COMMA sortsComponent )* )? EOF;

sortsComponent
    : metric (PIPE orderingValue)?;

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
    : ID EQUALS VALUE
    ;

orderingValue
    : ID
    ;
