// Fili Metric Definition Grammar for antlr4
//
// Note: this grammar produces a binary parse tree

grammar FiliMetric;

filiExpression
    : expression (FILTER filterExp)?
    ;

//
// Boolean filter logic
//
filterExp
    : andOrExp
    | equalExp
    ;

andOrExp
    : andOrExp (operator=(AND | OR) andOrArg)
    | andOrArg (operator=(AND | OR) andOrArg)
    ;

equalExp
    : (IDENTIFIER EQ QUOT IDENTIFIER QUOT)
    | (IDENTIFIER EQ anynum)
    ;

andOrArg
    : '(' filterExp ')'
    | equalExp
    ;

//
// Metric definition
//
expression
    : atom
    | mulDivExpression
    | plusMinusExpression
    ;

mulDivExpression
    : mulDivExpression (operator=(MUL | DIV) atom)
    | (atom operator=(MUL | DIV) atom)
    ;

plusMinusExpression
    : plusMinusExpression (operator=(PLUS | MINUS) plusMinusArg)
    | plusMinusArg (operator=(PLUS | MINUS) plusMinusArg)
    ;

plusMinusArg
    : atom
    | mulDivExpression
    ;

atom
    : anynum
    | function
    | IDENTIFIER
    | '(' expression ')'
    ;

function
    : IDENTIFIER '(' param_list ')'
    | IDENTIFIER '(' ')'               // Note: Not clear that this use case is valid
    ;

param_list
    : (expression COMMA)* expression   // Note: Not clear if this should be expression or identifier
    ;

anynum
    : (MINUS INTEGER | PLUS INTEGER | INTEGER)
    | (MINUS DECIMAL | PLUS DECIMAL | DECIMAL)
    ;

//
// Lexer types
//
PLUS : '+' ;
MINUS : '-' ;
MUL : '*' ;
DIV : '/' ;

FILTER : '|' ;
AND : '&&' ;
OR : '||' ;
EQ : '==' ;

DECIMAL : [0-9]* '.' [0-9]+ ;
INTEGER : [0-9]+ ;
IDENTIFIER : [a-zA-Z0-9_]+ ;

WHITESPACE : [ \t\r\n]+ -> skip ;
COMMA : ',';
QUOT : '"';
