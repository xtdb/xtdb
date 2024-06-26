grammar Sql;

options {
  language = Java;
  caseInsensitive = true;
}

BLOCK_COMMENT : '/*' ( BLOCK_COMMENT | . )*? '*/'  -> skip ;
LINE_COMMENT : '--' ~[\r\n]* -> skip ;

PLUS : '+' ;
MINUS : '-' ;
ASTERISK : '*' ;
SOLIDUS : '/' ;

TRUE : 'TRUE' ;
FALSE : 'FALSE' ;
NULL : 'NULL' ;

fragment DIGIT : [0-9] ;
fragment LETTER : [a-z] ;
fragment HEX_DIGIT : [0-9a-f] ;
fragment SIGN : '+' | '-' ;

WHITESPACE : [ \n\r\t]+ -> skip ;

/// SQL Keywords

ALL: 'ALL' ;
DISTINCT: 'DISTINCT' ;
NOT: 'NOT' ;
ASYMMETRIC: 'ASYMMETRIC' ;
SYMMETRIC: 'SYMMETRIC' ;
RECURSIVE: 'RECURSIVE' ;

/// §22 Direct invocation of SQL

/// §22.1 <direct SQL statement>

directSqlStatement : directlyExecutableStatement ';'? EOF ;

directlyExecutableStatement
    : queryExpression
    | insertStatement
    | updateStatementSearched
    | deleteStatementSearched
    | eraseStatementSearched
    | assertStatement
    | sqlTransactionStatement
    | sqlSessionStatement
    | setSessionVariableStatement
    | setValidTimeDefaults
    ;

setSessionVariableStatement : 'SET' identifier ( 'TO' | '=' ) literal ;
setValidTimeDefaults : 'SET' 'VALID_TIME_DEFAULTS' ( 'TO' | '=' )? validTimeDefaults ;

validTimeDefaults
  : 'AS_OF_NOW' #ValidTimeDefaultAsOfNow
  | 'ISO_STANDARD' #ValidTimeDefaultIsoStandard
  ;

//// §5 Lexical Elements

/// §5.3 Literals

literal
    : ('+' | '-')? UNSIGNED_FLOAT #FloatLiteral
    | ('+' | '-')? UNSIGNED_INTEGER #IntegerLiteral
    | characterString #CharacterStringLiteral
    | BINARY_STRING #BinaryStringLiteral
    | 'DATE' characterString #DateLiteral
    | 'TIME' characterString #TimeLiteral
    | 'TIMESTAMP' characterString #TimestampLiteral
    | 'INTERVAL' (PLUS | MINUS)? characterString intervalQualifier? #IntervalLiteral
    | 'DURATION' characterString #DurationLiteral
    | 'UUID' characterString #UUIDLiteral
    | (TRUE | FALSE) #BooleanLiteral
    | NULL #NullLiteral
    ;

fragment EXPONENT : 'E' SIGN? DIGIT+ ;
UNSIGNED_FLOAT : DIGIT+ ('.' DIGIT+ EXPONENT? | EXPONENT) ;
UNSIGNED_INTEGER : DIGIT+ ;

CHARACTER_STRING : '\'' ('\'\'' | ~('\''|'\n'))* '\'' ;
C_ESCAPES_STRING : 'E\''
  ( '\\' [0-7] [0-7]? [0-7]?
  | '\\x' HEX_DIGIT HEX_DIGIT
  | '\\u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
  | '\\U' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
  | '\\' ('b' | 'f' | 'n' | 'r' | 't' | '\\' | '\'' | '"')
  | '\'\''
  | ~('\\' | '\'')
  )*
  '\'' ;

characterString
    : CHARACTER_STRING #SqlStandardString
    | C_ESCAPES_STRING #CEscapesString
    ;

BINARY_STRING : 'X(\'' HEX_DIGIT* '\')' ;

intervalQualifier : startField 'TO' endField | singleDatetimeField ;
startField : nonSecondPrimaryDatetimeField ;
endField : singleDatetimeField ;
intervalFractionalSecondsPrecision : UNSIGNED_INTEGER ;
nonSecondPrimaryDatetimeField : 'YEAR' | 'MONTH' | 'DAY' | 'HOUR' | 'MINUTE' ;
singleDatetimeField : nonSecondPrimaryDatetimeField | 'SECOND' ( '(' intervalFractionalSecondsPrecision ')' )? ;

/// §5.4 Identifiers

identifierChain : identifier ( '.' identifier )* ;

identifier
    : (REGULAR_IDENTIFIER
        | 'START' | 'END'
        | 'AGE'
        | 'COMMITTED' | 'UNCOMMITTED'
        | setFunctionType )
        # RegularIdentifier
    | DELIMITED_IDENTIFIER # DelimitedIdentifier
    ;

POSTGRES_PARAMETER_SPECIFICATION : '$' DIGIT+ ;

REGULAR_IDENTIFIER : (LETTER | '$' | '_') (LETTER | DIGIT | '$' | '_' )* ;

DELIMITED_IDENTIFIER
    : '"' ('"' '"' | ~'"')* '"'
    | '`' ('`' '`' | ~'`')* '`'
    ;

schemaName : identifier ;
tableName : (identifier | schemaName '.' identifier) ;
columnName : identifier ;
correlationName : identifier ;

queryName : identifier ;
fieldName : identifier ;
windowName : identifier ;

// §6 Scalar Expressions

/// §6.1 Data Types

dataType
    : ('NUMERIC' | 'DECIMAL' | 'DEC') ('(' precision (',' scale)? ')')? # DecimalType
    | ('SMALLINT' | 'INTEGER' | 'INT' | 'BIGINT') # IntegerType
    | 'FLOAT' ('(' precision ')')? # FloatType
    | 'REAL' # RealType
    | 'DOUBLE' 'PRECISION' # DoubleType
    | 'BOOLEAN' # BooleanType
    | 'DATE' # DateType
    | 'TIME' ('(' precision ')')? withOrWithoutTimeZone? # TimeType
    | 'TIMESTAMP' ('(' precision ')')? withOrWithoutTimeZone? # TimestampType
    | 'INTERVAL' intervalQualifier? # IntervalType
    | 'VARCHAR' # CharacterStringType
    | 'DURATION' ('(' precision ')')? # DurationType
    | 'ROW' '(' fieldDefinition (',' fieldDefinition)* ')' # RowType
    | dataType 'ARRAY' ('[' maximumCardinality ']')? # ArrayType
    ;

precision : UNSIGNED_INTEGER ;
scale : UNSIGNED_INTEGER ;

charLengthUnits : 'CHARACTERS' | 'OCTETS' ;

withOrWithoutTimeZone
    : 'WITH' 'TIME' 'ZONE' #WithTimeZone
    | 'WITHOUT' 'TIME' 'ZONE' #WithoutTimeZone
    ;

maximumCardinality : UNSIGNED_INTEGER ;

fieldDefinition : fieldName dataType ;

/// §6.3 <value expression primary>

expr
    : expr 'IS' NOT? booleanValue #IsBooleanValueExpr
    | expr compOp expr # ComparisonPredicate
    | numericExpr NOT? 'BETWEEN' (ASYMMETRIC | SYMMETRIC)? numericExpr 'AND' numericExpr # BetweenPredicate
    | expr NOT? 'IN' inPredicateValue # InPredicate
    | expr 'NOT'? 'LIKE' likePattern ('ESCAPE' likeEscape)? # LikePredicate
    | expr 'NOT'? 'LIKE_REGEX' xqueryPattern ('FLAG' xqueryOptionFlag)? # LikeRegexPredicate
    | expr postgresRegexOperator xqueryPattern # PostgresRegexPredicate
    | expr 'IS' 'NOT'? 'NULL' # NullPredicate
    | expr compOp quantifier subquery # QuantifiedComparisonPredicate
    | 'NOT' expr #UnaryNotExpr
    | expr 'AND' expr #AndExpr
    | expr 'OR' expr #OrExpr
    | numericExpr #NumericExpr0
    ;

numericExpr
    : '+' numericExpr #UnaryPlusExpr
    | '-' numericExpr #UnaryMinusExpr
    | numericExpr (SOLIDUS | ASTERISK) numericExpr #NumericFactorExpr
    | numericExpr (PLUS | MINUS) numericExpr #NumericTermExpr
    | exprPrimary #ExprPrimary1
    ;

exprPrimary
    : '(' expr ')' #WrappedExpr
    | literal # LiteralExpr
    | exprPrimary '.' fieldName #FieldAccess
    | exprPrimary '[' expr ']' #ArrayAccess
    | exprPrimary '::' dataType #PostgresCastExpr
    | exprPrimary '||' exprPrimary #ConcatExpr

    | parameterSpecification # ParamExpr
    | columnReference # ColumnExpr
    | aggregateFunction # AggregateFunctionExpr
    | windowFunctionType 'OVER' windowNameOrSpecification # WindowFunctionExpr
    | nestedWindowFunction # NestedWindowFunctionExpr
    | subquery # ScalarSubqueryExpr
    | 'NEST_ONE' subquery # NestOneSubqueryExpr
    | 'NEST_MANY' subquery # NestManySubqueryExpr
    | 'CASE' expr simpleWhenClause+ elseClause? 'END' #SimpleCaseExpr
    | 'CASE' searchedWhenClause+ elseClause? 'END' # SearchedCaseExpr
    | 'NULLIF' '(' expr ',' expr ')' # NullIfExpr
    | 'COALESCE' '(' expr (',' expr)* ')' # CoalesceExpr
    | 'CAST' '(' expr 'AS' dataType ')' # CastExpr
    | arrayValueConstructor # ArrayExpr
    | objectConstructor # ObjectExpr

    | 'EXISTS' subquery # ExistsPredicate

    // period predicates
    | periodPredicand 'OVERLAPS' periodPredicand # PeriodOverlapsPredicate
    | periodPredicand 'EQUALS' periodPredicand # PeriodEqualsPredicate
    | periodPredicand 'CONTAINS' periodOrPointInTimePredicand # PeriodContainsPredicate
    | periodPredicand 'PRECEDES' periodPredicand # PeriodPrecedesPredicate
    | periodPredicand 'SUCCEEDS' periodPredicand # PeriodSucceedsPredicate
    | periodPredicand 'IMMEDIATELY' 'PRECEDES' periodPredicand # PeriodImmediatelyPrecedesPredicate
    | periodPredicand 'IMMEDIATELY' 'SUCCEEDS' periodPredicand # PeriodImmediatelySucceedsPredicate

    | (schemaName '.')? 'HAS_TABLE_PRIVILEGE' '('
        ( userString ',' )?
        tableString ','
        privilegeString
      ')' # HasTablePrivilegePredicate

    | (schemaName '.')? 'HAS_SCHEMA_PRIVILEGE' '('
        ( userString ',' )?
        schemaString ','
        privilegeString
      ')' # HasSchemaPrivilegePredicate

    // numeric value functions
    | 'POSITION' '(' expr 'IN' expr ( 'USING' charLengthUnits )? ')' # PositionFunction
    | 'EXTRACT' '(' extractField 'FROM' extractSource ')' # ExtractFunction
    | ('CHAR_LENGTH' | 'CHARACTER_LENGTH') '(' expr ('USING' charLengthUnits)? ')' # CharacterLengthFunction
    | 'OCTET_LENGTH' '(' expr ')' # OctetLengthFunction
    | 'LENGTH' '(' expr ')' # LengthFunction
    | 'CARDINALITY' '(' expr ')' # CardinalityFunction
    | 'ABS' '(' expr ')' # AbsFunction
    | 'MOD' '(' expr ',' expr ')' # ModFunction
    | trigonometricFunctionName '(' expr ')' # TrigonometricFunction
    | 'LOG' '(' generalLogarithmBase ',' generalLogarithmArgument ')' # LogFunction
    | 'LOG10' '(' expr ')' # Log10Function
    | 'LN' '(' expr ')' # LnFunction
    | 'EXP' '(' expr ')' # ExpFunction
    | 'POWER' '(' expr ',' expr ')' # PowerFunction
    | 'SQRT' '(' expr ')' # SqrtFunction
    | 'FLOOR' '(' expr ')' # FloorFunction
    | ( 'CEIL' | 'CEILING' ) '(' expr ')' # CeilingFunction
    | 'LEAST' '(' expr (',' expr)* ')' # LeastFunction
    | 'GREATEST' '(' expr (',' expr)* ')' # GreatestFunction

    // string value functions
    | 'SUBSTRING' '('
        expr
        'FROM' startPosition
        ( 'FOR' stringLength )?
        ( 'USING' charLengthUnits )?
      ')' # CharacterSubstringFunction

    | 'UPPER' '(' expr ')' # UpperFunction
    | 'LOWER' '(' expr ')' # LowerFunction
    | 'TRIM' '(' trimSpecification? trimCharacter? 'FROM'? trimSource ')' # TrimFunction

    | 'OVERLAY' '('
        expr
        'PLACING' expr
        'FROM' startPosition
        ( 'FOR' stringLength )?
        ( 'USING' charLengthUnits )?
      ')' # OverlayFunction

    | 'CURRENT_USER' # CurrentUserFunction
    | 'CURRENT_SCHEMA' # CurrentSchemaFunction
    | 'CURRENT_DATABASE' # CurrentDatabaseFunction

    | currentInstantFunction # CurrentInstantFunction0
    | endOfTimeFunction # EndOfTimeFunction0
    | 'CURRENT_TIME' ('(' precision ')')? # CurrentTimeFunction
    | 'LOCALTIME' ('(' precision ')')? # LocalTimeFunction
    | 'DATE_TRUNC' '(' dateTruncPrecision ',' dateTruncSource (',' dateTruncTimeZone)? ')' # DateTruncFunction

    // interval value functions
    | 'AGE' '(' expr ',' expr ')' # AgeFunction

    | 'TRIM_ARRAY' '(' expr ',' expr ')' # TrimArrayFunction
    ;

currentInstantFunction
    : 'CURRENT_DATE' ( '(' ')' )? # CurrentDateFunction
    | 'CURRENT_TIMESTAMP' ('(' precision ')')? # CurrentTimestampFunction
    | 'LOCALTIMESTAMP' ('(' precision ')')? # LocalTimestampFunction
    ;

endOfTimeFunction : 'END_OF_TIME' ('(' ')')? ;

booleanValue : 'TRUE' | 'FALSE' | 'UNKNOWN' ;

// spec addition: objectConstructor

objectConstructor
    : 'OBJECT' '(' (objectNameAndValue (',' objectNameAndValue)*)? ')'
    | '{' (objectNameAndValue (',' objectNameAndValue)*)? '}'
    ;

objectNameAndValue : objectName ':' expr ;

objectName : identifier ;

parameterSpecification
    : '?' #DynamicParameter
    | POSTGRES_PARAMETER_SPECIFICATION #PostgresParameter
    ;

columnReference : identifierChain ;

/// §6.10 <window function>

windowFunctionType
    : rankFunctionType '(' ')' # RankWindowFunction
    | 'ROW_NUMBER' '(' ')' # RowNumberWindowFunction
    | aggregateFunction # AggregateWindowFunction
    | 'NTILE' '(' numberOfTiles ')' # NtileWindowFunction

    | ('LEAD' | 'LAG') '('
        leadOrLagExtent
        (',' offset (',' defaultExpression)?)?
      ')' (nullTreatment)? # LeadOrLagWindowFunction

    | firstOrLastValue '(' expr ')' nullTreatment? # FirstOrLastValueWindowFunction
    | 'NTH_VALUE' '(' expr ',' nthRow ')' fromFirstOrLast? nullTreatment? # NthValueWindowFunction
;

rankFunctionType : 'RANK' | 'DENSE_RANK' | 'PERCENT_RANK' | 'CUME_DIST' ;

numberOfTiles : UNSIGNED_INTEGER | parameterSpecification ;

leadOrLagExtent : expr ;
offset : UNSIGNED_INTEGER ;
defaultExpression : expr ;

nullTreatment
    : 'RESPECT' 'NULLS' # RespectNulls
    | 'IGNORE' 'NULLS' # IgnoreNulls
    ;

firstOrLastValue : 'FIRST_VALUE' | 'LAST_VALUE' ;

nthRow : UNSIGNED_INTEGER | '?' ;

fromFirstOrLast
    : 'FROM' 'FIRST' # FromFirst
    | 'FROM' 'LAST' # FromLast
    ;

windowNameOrSpecification : windowName | windowSpecification ;

/// §6.11 <nested window function>

nestedWindowFunction
    : 'ROW_NUMBER' '(' rowMarker ')' # NestedRowNumberFunction
    | 'VALUE_OF' '(' expr 'AT' rowMarkerExpression (',' valueOfDefaultValue)? ')' # ValueOfExprAtRow
    ;

rowMarker : 'BEGIN_PARTITION' | 'BEGIN_FRAME' | 'CURRENT_ROW' | 'FRAME_ROW' | 'END_FRAME' | 'END_PARTITION' ;

rowMarkerExpression : rowMarker rowMarkerDelta? ;

rowMarkerDelta : PLUS rowMarkerOffset | MINUS rowMarkerOffset ;

rowMarkerOffset : UNSIGNED_INTEGER | '?' ;
valueOfDefaultValue : expr ;

/// §6.12 <case expression>

simpleWhenClause : 'WHEN' whenOperandList 'THEN' expr ;
searchedWhenClause : 'WHEN' expr 'THEN' expr ;
elseClause : 'ELSE' expr ;

whenOperandList : whenOperand (',' whenOperand)* ;
whenOperand : predicatePart2 | expr ;

/// §6.28 <numeric value function>

extractField : primaryDatetimeField | timeZoneField ;
primaryDatetimeField : nonSecondPrimaryDatetimeField | 'SECOND' ;
timeZoneField : 'TIMEZONE_HOUR' | 'TIMEZONE_MINUTE' ;
extractSource : expr ;

/// §6.30 <string value function>

trimSource : expr ;
trimSpecification : 'LEADING' | 'TRAILING' | 'BOTH' ;
trimCharacter : expr ;

startPosition : expr ;
stringLength : expr ;

/// §6.32 <datetime value function>

// spec additions for date_trunc

dateTruncPrecision
    : 'MILLENNIUM' | 'CENTURY' | 'DECADE'
    | 'YEAR' | 'QUARTER' | 'MONTH' | 'WEEK' | 'DAY'
    | 'HOUR' | 'MINUTE' | 'SECOND'
    | 'MILLISECOND' | 'MICROSECOND' | 'NANOSECOND'
    ;

dateTruncSource : expr ;
dateTruncTimeZone : characterString ;

/// §6.34 <interval value function>

// spec addition: age function

/// §6.37 <array value function>

/// §6.38 <array value constructor>

arrayValueConstructor
    : 'ARRAY'? '[' (expr (',' expr)*)? ']' # ArrayValueConstructorByEnumeration
    | 'ARRAY' subquery # ArrayValueConstructorByQuery
    ;

/// SQL:2016 §6.30 Trigonometric functions

trigonometricFunctionName : 'SIN' | 'COS' | 'TAN' | 'SINH' | 'COSH' | 'TANH' | 'ASIN' | 'ACOS' | 'ATAN' ;

// general_logarithm_function

generalLogarithmBase : expr ;
generalLogarithmArgument : expr ;

//// §7 Query expressions

/// §7.1 <row value constructor>

rowValueConstructor
    : expr # SingleExprRowConstructor
    | '(' ( expr (',' expr)+ )? ')'  # MultiExprRowConstructor
    | 'ROW' '(' ( expr (',' expr)* )? ')' # MultiExprRowConstructor
    ;

/// §7.3 <table value constructor>

rowValueList : rowValueConstructor (',' rowValueConstructor)* ;
tableValueConstructor : 'VALUES' rowValueList ;

/// §7.5 <from clause>

fromClause : 'FROM' tableReference (',' tableReference)* ;

/// §7.6 <table reference>

tableReference
    : tableOrQueryName (querySystemTimePeriodSpecification | queryValidTimePeriodSpecification)* tableAlias? tableProjection? # BaseTable
    | tableReference joinType? 'JOIN' tableReference joinSpecification # JoinTable
    | tableReference 'CROSS' 'JOIN' tableReference # CrossJoinTable
    | tableReference 'NATURAL' joinType? 'JOIN' tableReference # NaturalJoinTable
    | subquery tableAlias tableProjection? # DerivedTable
    | 'LATERAL' subquery tableAlias tableProjection? # LateralDerivedTable
    | 'UNNEST' '(' expr ')' withOrdinality? tableAlias tableProjection? # CollectionDerivedTable
    | 'ARROW_TABLE' '(' characterString ')' tableAlias tableProjection # ArrowTable
    | '(' tableReference ')' # WrappedTableReference
    ;

withOrdinality : ('WITH' 'ORDINALITY') ;

tableAlias : 'AS'? correlationName ;
tableProjection : '(' columnNameList ')' ;

querySystemTimePeriodSpecification
    : 'FOR' 'SYSTEM_TIME' tableTimePeriodSpecification
    | 'FOR' ALL 'SYSTEM_TIME'
    ;

queryValidTimePeriodSpecification
   : 'FOR' 'VALID_TIME' tableTimePeriodSpecification
   | 'FOR' ALL 'VALID_TIME'
   ;

tableTimePeriodSpecification
    : 'AS' 'OF' periodSpecificationExpr # TableAsOf
    | 'ALL' # TableAllTime
    | 'BETWEEN' periodSpecificationExpr 'AND' periodSpecificationExpr # TableBetween
    | 'FROM' periodSpecificationExpr 'TO' periodSpecificationExpr # TableFromTo
    ;

periodSpecificationExpr
    : literal #PeriodSpecLiteral
    | parameterSpecification #PeriodSpecParam
    | currentInstantFunction #PeriodSpecCurrentInstant
    | endOfTimeFunction #PeriodSpecEndOfTime
    ;

tableOrQueryName : tableName ;

columnNameList : columnName (',' columnName)* ;

/// §7.7 <joined table>

joinSpecification
    : 'ON' expr # JoinCondition
    | 'USING' '(' columnNameList ')' # NamedColumnsJoin
    ;

joinType : 'INNER' | outerJoinType 'OUTER'? ;
outerJoinType : 'LEFT' | 'RIGHT' | 'FULL' ;

/// §7.8 <where clause>

whereClause : 'WHERE' expr ;

/// §7.9 <group by clause>

groupByClause : 'GROUP' 'BY' (setQuantifier)? groupingElement (',' groupingElement)* ;

groupingElement
    : columnReference # OrdinaryGroupingSet
    | '(' ')' # EmptyGroupingSet
    ;

/// §7.10 <having clause>

havingClause : 'HAVING' expr ;

/// §7.11 <window clause>

windowClause : 'WINDOW' windowDefinitionList ;
windowDefinitionList : windowDefinition (',' windowDefinition)* ;
windowDefinition : newWindowName 'AS' windowSpecification ;
newWindowName : windowName ;

windowSpecification : '(' windowSpecificationDetails ')' ;

windowSpecificationDetails : (existingWindowName)? (windowPartitionClause)? (windowOrderClause)? (windowFrameClause)? ;

existingWindowName : windowName ;
windowPartitionClause : 'PARTITION' 'BY' windowPartitionColumnReferenceList ;
windowPartitionColumnReferenceList : windowPartitionColumnReference (',' windowPartitionColumnReference)* ;
windowPartitionColumnReference : columnReference ;
windowOrderClause : 'ORDER' 'BY' sortSpecificationList ;
windowFrameClause : windowFrameUnits windowFrameExtent (windowFrameExclusion)? ;
windowFrameUnits : 'ROWS' | 'RANGE' | 'GROUPS' ;
windowFrameExtent : windowFrameStart | windowFrameBetween ;
windowFrameStart : 'UNBOUNDED' 'PRECEDING' | windowFramePreceding | 'CURRENT' 'ROW' ;
windowFramePreceding : UNSIGNED_INTEGER 'PRECEDING' ;
windowFrameBetween : 'BETWEEN' windowFrameBound1 'AND' windowFrameBound2 ;
windowFrameBound1 : windowFrameBound ;
windowFrameBound2 : windowFrameBound ;
windowFrameBound : windowFrameStart | 'UNBOUNDED' 'FOLLOWING' | windowFrameFollowing ;
windowFrameFollowing : UNSIGNED_INTEGER 'FOLLOWING' ;

windowFrameExclusion : 'EXCLUDE' 'CURRENT' 'ROW' | 'EXCLUDE' 'GROUP' | 'EXCLUDE' 'TIES' | 'EXCLUDE' 'NO' 'OTHERS' ;

/// §7.12 <query specification>

selectClause : 'SELECT' setQuantifier? selectList ;
selectList : (selectListAsterisk | selectSublist) (',' selectSublist)* ;

selectListAsterisk : ASTERISK excludeClause? renameClause? ;

selectSublist : derivedColumn | qualifiedAsterisk ;
qualifiedAsterisk : identifierChain '.' ASTERISK excludeClause? qualifiedRenameClause?;

renameClause
    : 'RENAME' renameColumn
    | 'RENAME' '(' renameColumn (',' renameColumn )* ')'
    ;

renameColumn : columnReference asClause ;

qualifiedRenameClause
    : 'RENAME' qualifiedRenameColumn
    | 'RENAME' '(' qualifiedRenameColumn (',' qualifiedRenameColumn )* ')'
    ;

qualifiedRenameColumn : identifier asClause ;

excludeClause
    : 'EXCLUDE' identifier
    | 'EXCLUDE' '(' identifier (',' identifier )* ')'
    ;

derivedColumn : expr asClause? ;
asClause : 'AS'? columnName ;

/// §7.13 <query expression>

queryExpression : withClause? queryExpressionBody orderByClause? resultOffsetClause? fetchFirstClause? ;
withClause : 'WITH' RECURSIVE? withListElement (',' withListElement)* ;
withListElement : queryName ('(' columnNameList ')')? 'AS' subquery ;

queryExpressionBody
    : queryTerm # QueryBodyTerm
    | queryExpressionBody 'UNION' (ALL | DISTINCT)? queryTerm # UnionQuery
    | queryExpressionBody 'EXCEPT' (ALL | DISTINCT)? queryTerm # ExceptQuery
    ;

queryTerm
    : selectClause fromClause? whereClause? groupByClause? havingClause? windowClause? # QuerySpecification
    | fromClause whereClause? groupByClause? havingClause? selectClause? windowClause? # QuerySpecification
    | tableValueConstructor # ValuesQuery
    | '(' queryExpressionBody ')' # WrappedQuery
    | queryTerm 'INTERSECT' (ALL | DISTINCT)? queryTerm # IntersectQuery
    ;
    

orderByClause : 'ORDER' 'BY' sortSpecificationList ;
resultOffsetClause : 'OFFSET' offsetRowCount ( 'ROW' | 'ROWS' )? ;

fetchFirstClause
    : 'FETCH' ('FIRST' | 'NEXT') fetchFirstRowCount? ( 'ROW' | 'ROWS' ) 'ONLY'
    | 'LIMIT' fetchFirstRowCount
    ;

offsetRowCount : UNSIGNED_INTEGER | parameterSpecification ;
fetchFirstRowCount : UNSIGNED_INTEGER | parameterSpecification ;

/// §7.15 <subquery>

subquery : '(' queryExpression ')' ;

//// §8 Predicates

predicatePart2
    : compOp expr # ComparisonPredicatePart2
    | NOT? 'BETWEEN' (ASYMMETRIC | SYMMETRIC)? expr 'AND' expr # BetweenPredicatePart2
    | NOT? 'IN' inPredicateValue # InPredicatePart2
    | 'NOT'? 'LIKE' likePattern ('ESCAPE' likeEscape)? # LikePredicatePart2
    | 'NOT'? 'LIKE_REGEX' xqueryPattern ('FLAG' xqueryOptionFlag)? # LikeRegexPredicatePart2
    | postgresRegexOperator xqueryPattern # PostgresRegexPredicatePart2
    | 'IS' 'NOT'? 'NULL' # NullPredicatePart2
    | compOp quantifier subquery # QuantifiedComparisonPredicatePart2
    ;

compOp : '=' | '!=' | '<>' | '<' | '>' | '<=' | '>=' ;

inPredicateValue
    : subquery # InSubquery
    | '(' rowValueList ')' # InRowValueList
    ;

likePattern : exprPrimary ;
likeEscape : exprPrimary ;

xqueryPattern : exprPrimary ;
xqueryOptionFlag : exprPrimary ;

postgresRegexOperator : '~' | '~*' | '!~' | '!~*' ;

quantifier : 'ALL' | 'SOME' | 'ANY' ;

periodPredicand
    : (tableName '.')? periodColumnName # PeriodColumnReference
    | 'PERIOD' '(' periodStartValue ',' periodEndValue ')' # PeriodValueConstructor
    ;

periodColumnName : 'VALID_TIME' | 'SYSTEM_TIME' ;
periodStartValue : expr ;
periodEndValue : expr ;

periodOrPointInTimePredicand : periodPredicand | pointInTimePredicand ;
pointInTimePredicand : expr ;
/// §8.21 <search condition>

searchCondition : expr ;

/// postgres access privilege predicates

userString : expr ;
tableString : expr ;
schemaString : expr ;
privilegeString : expr ;

//// §10 Additional common elements

/// §10.9 <aggregate function>

aggregateFunction
    : 'COUNT' '(' ASTERISK ')' # CountStarFunction
    | 'ARRAY_AGG' '(' expr ('ORDER' 'BY' sortSpecificationList)? ')' # ArrayAggFunction
    | setFunctionType '(' setQuantifier? expr ')' # SetFunction
    ;

setFunctionType
    : 'AVG' | 'MAX' | 'MIN' | 'SUM' | 'COUNT'
    | 'EVERY' | 'ANY' | 'SOME'
    | 'STDDEV_POP' | 'STDDEV_SAMP' | 'VAR_SAMP' | 'VAR_POP' ;

setQuantifier : 'DISTINCT' | 'ALL' ;

/// §10.10 <sort specification list>

sortSpecificationList : sortSpecification (',' sortSpecification)* ;
sortSpecification : expr orderingSpecification? nullOrdering? ;
orderingSpecification : 'ASC' | 'DESC' ;
nullOrdering : 'NULLS' 'FIRST' | 'NULLS' 'LAST' ;

/// §13.4 <SQL procedure statement>

sqlTransactionStatement
    : ('START' 'TRANSACTION' | 'BEGIN') transactionCharacteristics? # StartTransactionStatement
    | 'SET' 'LOCAL'? 'TRANSACTION' transactionCharacteristics # SetTransactionStatement
    | 'COMMIT' # CommitStatement
    | 'ROLLBACK' # RollbackStatement
    ;

sqlSessionStatement
    : 'SET' 'SESSION' 'CHARACTERISTICS' 'AS' sessionCharacteristic (',' sessionCharacteristic)* # SetSessionCharacteristicsStatement
    | 'SET' 'TIME' 'ZONE' characterString # SetTimeZoneStatement
    ;

sessionCharacteristic : 'TRANSACTION' transactionMode (',' transactionMode)* ;

/// §14 Data manipulation

/// §14.9 <delete statement: searched>

deleteStatementSearched
    : 'DELETE' 'FROM' tableName
      dmlStatementValidTimeExtents?
      ( 'AS'? correlationName )?
      ( 'WHERE' searchCondition )?
    ;

dmlStatementValidTimeExtents : dmlStatementValidTimePortion | dmlStatementValidTimeAll ;
dmlStatementValidTimePortion : 'FOR' 'PORTION' 'OF' 'VALID_TIME' 'FROM' expr 'TO' expr ;
dmlStatementValidTimeAll : 'FOR' 'ALL' 'VALID_TIME' | 'FOR' 'VALID_TIME' 'ALL' ;
eraseStatementSearched : 'ERASE' 'FROM' tableName ( 'AS'? correlationName )? ('WHERE' searchCondition)? ;

/// §14.11 <insert statement>

insertStatement : 'INSERT' 'INTO' tableName insertColumnsAndSource ;
insertColumnsAndSource
    : ( '(' columnNameList ')' )? tableValueConstructor # InsertValues
    | ( '(' columnNameList ')' )? queryExpression # InsertFromSubquery
    ;

/// §14.14 <update statement: searched>

updateStatementSearched
    : 'UPDATE' tableName
      dmlStatementValidTimeExtents?
      ( 'AS'? correlationName )?
      'SET' setClauseList
      ( 'WHERE' searchCondition )?
    ;

/// §14.15 <set clause list>

setClauseList : setClause (',' setClause)* ;
setClause : setTarget '=' updateSource ;

// TODO SQL:2011 supports updating keys within a struct here
setTarget : columnName ;

updateSource : expr ;

assertStatement : 'ASSERT' searchCondition ;

/// §17.3 <transaction characteristics>

transactionCharacteristics : transactionMode (',' transactionMode)* ;
transactionMode
    : 'ISOLATION' 'LEVEL' levelOfIsolation  # IsolationLevel
    | 'READ' 'ONLY' # ReadOnlyTransaction
    | 'READ' 'WRITE' # ReadWriteTransaction ;

levelOfIsolation
    : 'READ' 'UNCOMMITTED' # ReadUncommittedIsolation
    | 'READ' 'COMMITTED' # ReadCommittedIsolation
    | 'REPEATABLE' 'READ' # RepeatableReadIsolation
    | 'SERIALIZABLE' # SerializableIsolation
    ;

