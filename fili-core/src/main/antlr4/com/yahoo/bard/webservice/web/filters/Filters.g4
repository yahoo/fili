grammar Filters;

options {
    tokenVocab=FiltersLex;
}

// filter format: dimension|field-op[value,value,...],dim2|field2-op2[value,value,...]...

filters : filter ( COMMA filter)* ;

filter : dimension PIPE field DASH op OPEN_BRACKET filter_values CLOSE_BRACKET ;

dimension : ID;

field : ID ;

op : ID ;

filter_values : VALUE (COMMA VALUE)* ;