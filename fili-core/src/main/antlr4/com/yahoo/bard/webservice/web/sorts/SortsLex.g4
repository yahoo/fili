lexer grammar SortsLex;

options { }

tokens { OPERATOR, VALUE, COMMA, PIPE }

// reused from metric
WS : [ \t\n\r]+ -> skip;
OPEN_PARENTHESIS : '(' ;
CLOSE_PARENTHESIS : ')' ;
COMMA : ',';
ID : [a-zA-Z0-9_]+;

EQUALS: '=' -> pushMode(VALUE_MODE);

// Quoted String framents
fragment ESC : '\\"' | '\\\\' ; // 2 char sequences \" and \\
fragment QUOTED_STRING : '"' (ESC|.)*? '"' ;

// Numeric value fragments
fragment DASH : '-' ;
fragment DOT: '.' ;
fragment DIGITS : [0-9] ;
fragment SCIENTIFIC_NOTATION : [eE] ;
fragment SIGNS : [+\-] ;

PIPE : '|';


mode VALUE_MODE;

UNQUOTED_VALUE : [a-zA-Z0-9_]+ -> type(VALUE), popMode;

QUOTED_VALUE : QUOTED_STRING -> type(VALUE), popMode;

NUMERIC_VALUE : ((  SIGNS? DIGITS+ (DOT DIGITS*)? ( SCIENTIFIC_NOTATION SIGNS? DIGITS+) ?
                | SIGNS? DOT DIGITS+ ( SCIENTIFIC_NOTATION SIGNS? DIGITS+ )?
               ) {setText(getText().trim());} ) -> type(VALUE), popMode;

