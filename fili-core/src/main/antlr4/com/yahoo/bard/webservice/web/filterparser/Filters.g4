grammar Filters;

options {
    tokenVocab=FiltersLex;
}

filters 
    : ( filterComponent ( COMMA filterComponent )* )? EOF;

filter
    : filterComponent EOF;

filterComponent 
    : dimension PIPE field DASH OPERATOR OPEN_BRACKET values CLOSE_BRACKET;

dimension
    : ID
    ;

field
    : ID
    ;

values
    : VALUE ( COMMA VALUE )*
    ;
