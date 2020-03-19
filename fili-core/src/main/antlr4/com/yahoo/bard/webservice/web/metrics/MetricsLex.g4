lexer grammar MetricsLex;
options { }
tokens { OPERATOR, PARAMETER, KEY, VALUE }
WS : [ \t\n\r]+ -> skip;
OPEN_PARENTHESIS : '(' ;
CLOSE_PARENTHESIS : ')' ;
COMMA : ',';
ID : [a-zA-Z0-9_]+;
EQUALS: '=' -> pushMode(VALUE_MODE);
// values mode
mode VALUE_MODE;
fragment DASH : '-';
fragment DOT: '.' ;
fragment DIGITS : [0-9];
fragment SCIENTIFIC_NOTATION : [eE];
fragment SIGNS : [+\-];
NUMERIC_VALUE : ((  SIGNS? DIGITS+ (DOT DIGITS*)? ( SCIENTIFIC_NOTATION SIGNS? DIGITS+) ?
                | SIGNS? DOT DIGITS+ ( SCIENTIFIC_NOTATION SIGNS? DIGITS+ )?
               ) {setText(getText().trim());} ) -> type(VALUE), popMode;
TEXT_VALUE : ID -> type(VALUE), popMode;
