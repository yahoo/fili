grammar Havings;

options {
    tokenVocab=HavingsLex;
}

havings
    : ( havingComponent ( COMMA havingComponent )* )? EOF;

havingComponent
    : metric DASH OPERATOR OPEN_BRACKET values CLOSE_BRACKET;

metric
    : ID ( OPEN_PARENTHESIS ID EQUALS ID CLOSE_PARENTHESIS )?
    ;

values
    : VALUE ( COMMA VALUE )*
    ;
