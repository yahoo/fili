lexer grammar MetricsLex;

options { }

tokens { OPERATOR, PARAMETER, KEY, VALUE, ESCAPED_VALUE }

// Numeric value fragments
fragment DOT: '.' ;
fragment DIGITS : [0-9] ;
fragment SCIENTIFIC_NOTATION : [eE] ;
fragment SIGNS : [+\-] ;

OPEN_PARENTHESIS : '(' -> pushMode(PARAM_MODE);
CLOSE_PARENTHESIS : ')' ;
COMMA : ',';
DASH: '-';

ID : [a-zA-Z0-9_]+;
WS : [ \t\n\r]+ -> skip;

// Having Operators
mode PARAM_MODE;
WS0 : [ \t\n\r]+ -> skip;
COMMA0 : ',' -> type(COMMA);
ID2 : [a-zA-Z0-9_|]+ -> type(ID);
EQUALS: '=' -> pushMode(VALUE_MODE);

CLOSE_PARENTHESIS2 : ')' -> type(CLOSE_PARENTHESIS), popMode ;

mode VALUE_MODE;
WS1 : [ \t\n\r]+ -> skip;

COMMA1 : ',' -> type(COMMA) ;

// I don't think you need to escape \, only '
ESCAPED_VALUE : '(' ( '\\)' | ~[)] )* ')' -> type(ESCAPED_VALUE), popMode;

UNESCAPED_VALUE : [a-zA-Z0-9_]+ -> type(VALUE), popMode;


