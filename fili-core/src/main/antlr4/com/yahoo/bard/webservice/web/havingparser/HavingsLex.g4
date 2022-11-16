lexer grammar HavingsLex;

options { }

tokens { OPERATOR, PARAMETER, KEY, VALUE, ESCAPED_VALUE, HAVING_VALUE }

// Reused from metric

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


OPEN_BRACKET : '[' -> pushMode(HAVING_VALUE_MODE);
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


// end of reused from metric
// Having specific syntax

// having values mode
mode HAVING_VALUE_MODE;
WS2 : [ \t\n\r]+ -> skip;

COMMA2 : ',' -> type(COMMA);

NUMERIC_VALUE : (  SIGNS? DIGITS+ (DOT DIGITS*)? ( SCIENTIFIC_NOTATION SIGNS? DIGITS+) ?
                | SIGNS? DOT DIGITS+ ( SCIENTIFIC_NOTATION SIGNS? DIGITS+ )?
               ) {setText(getText().trim());} -> type(HAVING_VALUE);

CLOSE_BRACKET : ']' -> popMode;

