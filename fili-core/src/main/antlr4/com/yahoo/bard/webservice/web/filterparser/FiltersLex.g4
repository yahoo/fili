lexer grammar FiltersLex;

options { }

tokens { OPERATOR, VALUE, COMMA }

COMMA1 : ',' -> type(COMMA);
PIPE : '|';
DASH : '-';
OPEN_BRACKET : '[' -> pushMode(VALUE_MODE);
IN : 'in' -> type(OPERATOR);
NOTIN : 'notin' -> type(OPERATOR);
CONTAINS : 'contains' -> type(OPERATOR);
STARTS_WITH : ( 'startsWith' | 'startswith' ) -> type(OPERATOR);
EQ : 'eq' -> type(OPERATOR);

ID : [a-zA-Z0-9_]+;


// values mode
mode VALUE_MODE;
WS : [ \t\n\r]+ -> skip;
SIMPLE_VALUE : ~[\]," \t\n\r] ( ~[\],] )* {setText(getText().trim());} -> type(VALUE);
QUOTED_VALUE : ( '"' ( ~["] | '""' )* '"' ) {String t = getText(); setText(t.substring(1,t.length()-1).replaceAll("\"\"","\""));} -> type(VALUE);
CLOSE_BRACKET : ']' -> popMode;
COMMA2 : ',' -> type(COMMA);
