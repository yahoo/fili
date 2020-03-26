lexer grammar HavingsLex;

options { }

tokens { OPERATOR, VALUE, COMMA }

COMMA1 : ',' -> type(COMMA);
DASH : '-';
EQUALS: '=';
DOT: '.' ;
OPEN_PARENTHESIS : '(';
CLOSE_PARENTHESIS : ')';
OPEN_BRACKET : '[' -> pushMode(VALUE_MODE);

EQUAL_TO : ( 'equalTo'
           | 'equals'
           | 'eq'
           ) -> type(OPERATOR);

GREATER_THAN : ( 'greaterThan'
               | 'greater'
               | 'gt'
               ) -> type(OPERATOR);

LESSER_THAN : ( 'lessThan'
              | 'less'
              | 'lt'
              ) -> type(OPERATOR);

NOT_EQUAL_TO : ( 'notEqualTo'
               | 'notEquals'
               | 'noteq'
               | 'neq'
               ) -> type(OPERATOR);

NOT_GREATER_THAN : ( 'notGreaterThan'
                   | 'notGreater'
                   | 'notgt'
                   | 'lte'
                   ) -> type(OPERATOR);

NOT_LESS_THAN : ( 'notLessThan'
                | 'notLess'
                | 'notlt'
                | 'gte'
                ) -> type(OPERATOR);

BETWEEN : ( 'between'
          | 'bet'
          ) -> type(OPERATOR);

NOT_BETWEEN : ( 'notBetween'
              | 'nbet'
              ) -> type(OPERATOR);

ID : [a-zA-Z0-9_]+;
fragment DIGITS : [0-9];
fragment SCIENTIFIC_NOTATION : [eE];
fragment SIGNS : [+\-];

// values mode
mode VALUE_MODE;
WS : [ \t\n\r]+ -> skip;
SIMPLE_VALUE : (  SIGNS? DIGITS+ (DOT DIGITS*)? ( SCIENTIFIC_NOTATION SIGNS? DIGITS+) ?
                | SIGNS? DOT DIGITS+ ( SCIENTIFIC_NOTATION SIGNS? DIGITS+ )?
               ) {setText(getText().trim());} -> type(VALUE);
//TEXT_VALUE : ID+ -> type(VALUE);

CLOSE_BRACKET : ']' -> popMode;
COMMA2 : ',' -> type(COMMA);

