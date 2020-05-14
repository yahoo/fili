lexer grammar FiltersLex;

options { }

// TODO what does this do? Is it necessary?
tokens { DASH, PIPE, COMMA, OPEN_BRACKET, CLOSE_BRACKET, ID, VALUE }

// filter format: dimension|field-op[value,value,...],dim2|field2-op2[value,value,...]...

DASH : '-';
PIPE : '|';
COMMA : ',';

OPEN_BRACKET : '[' -> pushMode(VALUE_MODE);
CLOSE_BRACKET : ']' ;

ID : [a-zA-Z0-9_]+;

mode VALUE_MODE;

COMMA0: ',' -> type(COMMA);
VALUE: ~[,[\]]+ ;
CLOSE_BRACKET0 : ']' -> popMode, type(CLOSE_BRACKET);