lexer grammar MetricsLex;

options { }

tokens { OPERATOR, PARAMETER, KEY, VALUE }

OPEN_PARENTHESIS : '(' -> pushMode(PARAN_MODE);
CLOSE_PARENTHESIS : ')' ;
COMMA : ',';
DASH: '-';

// Numeric value fragments
fragment DOT: '.' ;
fragment DIGITS : [0-9] ;
fragment SCIENTIFIC_NOTATION : [eE] ;
fragment SIGNS : [+\-] ;

ID : [a-zA-Z0-9_]+;
WS : [ \t\n\r]+ -> skip;

// Having Operators
mode PARAN_MODE;
WS0 : [ \t\n\r]+ -> skip;
COMMA0 : (',' | ',') -> type(COMMA);
ID2 : [a-zA-Z0-9_]+ -> type(ID);
EQUALS: '=' -> pushMode(VALUE_MODE);

CLOSE_PARENTHESIS2 : ')' -> type(CLOSE_PARENTHESIS), popMode ;

mode VALUE_MODE;
WS1 : [ \t\n\r]+ -> skip;

COMMA1 : (',' | ', ') -> type(COMMA);

UNQUOTED_VALUE : [a-zA-Z0-9_]+ -> type(VALUE), popMode;

NUMERIC_VALUE : ((  SIGNS? DIGITS+ (DOT DIGITS*)? ( SCIENTIFIC_NOTATION SIGNS? DIGITS+) ?
                | SIGNS? DOT DIGITS+ ( SCIENTIFIC_NOTATION SIGNS? DIGITS+ )?
               ) {setText(getText().trim());} ) -> type(VALUE), popMode;
