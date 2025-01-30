parser grammar Sql;

options {
  language = Java;
  caseInsensitive = true;
  tokenVocab = SqlLexer;
}

/// §22 Direct invocation of SQL

/// §22.1 <direct SQL statement>

directSqlStatement : directlyExecutableStatement ';'? EOF ;
multiSqlStatement : directlyExecutableStatement ( ';' directlyExecutableStatement )* ';'? EOF ;

directlyExecutableStatement
    : settingQueryVariables? queryExpression #QueryExpr
    | insertStatement #InsertStmt
    | updateStatementSearched #UpdateStmt
    | deleteStatementSearched #DeleteStmt
    | patchStatement #PatchStmt
    | eraseStatementSearched #EraseStmt
    | prepareStatement #PrepareStmt
    | executeStatement #ExecuteStmt
    | ASSERT searchCondition #AssertStatement
    | (START TRANSACTION | BEGIN) transactionCharacteristics? # StartTransactionStatement
    | SET TRANSACTION ISOLATION LEVEL levelOfIsolation # SetTransactionStatement
    | COMMIT # CommitStatement
    | ROLLBACK # RollbackStatement
    | SET SESSION CHARACTERISTICS AS sessionCharacteristic (',' sessionCharacteristic)* # SetSessionCharacteristicsStatement
    | SET ROLE ( identifier | NONE ) # SetRoleStatement
    | SET TIME ZONE characterString # SetTimeZoneStatement
    | SET WATERMARK ( TO | '=' ) literal # SetWatermarkStatement
    | SET identifier ( TO | '=' ) literal # SetSessionVariableStatement
    | SHOW showVariable # ShowVariableStatement
    | SHOW identifier # ShowSessionVariableStatement
    | SHOW WATERMARK # ShowWatermarkStatement
    | CREATE USER userName WITH PASSWORD password=characterString # CreateUserStatement
    | ALTER USER userName WITH PASSWORD password=characterString # AlterUserStatement
    ;

executeStatement : EXECUTE statementName=identifier executeArgs ;

executeArgs : ('(' expr (',' expr)* ')')? ;

prepareStatement : PREPARE statementName=identifier AS directlyExecutableStatement ;

showVariable
   : 'TRANSACTION' 'ISOLATION' 'LEVEL' # ShowTransactionIsolationLevel
   | ('TIME' 'ZONE' | 'TIMEZONE') # ShowTimeZone
   ;

settingQueryVariables : 'SETTING' settingQueryVariable (',' settingQueryVariable)* ;

settingQueryVariable
    : 'DEFAULT' 'VALID_TIME' 'TO'? tableTimePeriodSpecification # SettingDefaultValidTime
    | 'DEFAULT' 'SYSTEM_TIME' 'TO'? tableTimePeriodSpecification # SettingDefaultSystemTime
    | 'SNAPSHOT_TIME' ('TO' | '=') snapshotTime=literal # SettingSnapshotTime
    | 'CURRENT_TIME' ('TO' | '=') currentTime=literal # SettingCurrentTime
    ;

//// §5 Lexical Elements

/// §5.3 Literals

intervalLiteral : 'INTERVAL' (PLUS | MINUS)? characterString intervalQualifier? ;

dateTimeLiteral
    : 'DATE' characterString #DateLiteral
    | 'TIMESTAMP' withOrWithoutTimeZone? characterString #TimestampLiteral
    ;

literal
    : ('+' | '-')? UNSIGNED_FLOAT #FloatLiteral
    | ('+' | '-')? UNSIGNED_INTEGER #IntegerLiteral
    | characterString #CharacterStringLiteral
    | BINARY_STRING #BinaryStringLiteral
    | dateTimeLiteral #DateTimeLiteral0
    | 'TIME' characterString #TimeLiteral
    | intervalLiteral #IntervalLiteral0
    | 'DURATION' characterString #DurationLiteral
    | 'UUID' characterString #UUIDLiteral
    | 'URI' characterString #URILiteral
    | 'KEYWORD' characterString #KeywordLiteral
    | (TRUE | FALSE | ON | OFF) #BooleanLiteral
    | NULL #NullLiteral
    ;

dollarStringText: DM_TEXT+ ;

characterString
    : CHARACTER_STRING #SqlStandardString
    | C_ESCAPES_STRING #CEscapesString
    | DOLLAR_TAG dollarStringText? DM_END_TAG #DollarString
    ;

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
        | 'TIMEZONE'
        | 'VERSION'
        | 'SYSTEM_TIME' | 'VALID_TIME'
        | 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'ERASE'
        | 'SETTING'
        | 'CARDINALITY'
        | 'ROLE'
        | 'STR' | 'LOCAL_NAME' | 'NAMESPACE'
        | 'USER' | 'PASSWORD'
        | 'VARBINARY' | 'BYTEA'
        | 'URI'
        | setFunctionType )
        # RegularIdentifier
    | DELIMITED_IDENTIFIER # DelimitedIdentifier
    ;

schemaName : identifier ;
tableName : (identifier | schemaName '.' identifier) ;
columnName : identifier ;
correlationName : identifier ;

queryName : identifier ;
fieldName : identifier ;
windowName : identifier ;
userName: identifier;

// §6 Scalar Expressions

/// §6.1 Data Types

dataType
    : ('NUMERIC' | 'DECIMAL' | 'DEC') ('(' precision (',' scale)? ')')? # DecimalType
    | ('SMALLINT' | 'INTEGER' | 'INT' | 'BIGINT') # IntegerType
    | 'FLOAT' ('(' precision ')')? # FloatType
    | 'REAL' # RealType
    | 'DOUBLE' 'PRECISION'? # DoubleType
    | 'BOOLEAN' # BooleanType
    | 'DATE' # DateType
    | 'TIME' ('(' precision ')')? withOrWithoutTimeZone? # TimeType
    | 'TIMESTAMP' ('(' precision ')')? withOrWithoutTimeZone? # TimestampType
    | 'TIMESTAMPTZ' #TimestampTzType
    | 'INTERVAL' intervalQualifier? # IntervalType
    | ('VARCHAR' | 'TEXT') # CharacterStringType
    | 'DURATION' ('(' precision ')')? # DurationType
    | 'ROW' '(' fieldDefinition (',' fieldDefinition)* ')' # RowType
    | 'REGCLASS' #RegClassType
    | 'REGPROC' #RegProcType
    | 'KEYWORD' #KeywordType
    | 'UUID' #UuidType
    | 'VARBINARY' #VarbinaryType
    | 'BYTEA' #VarbinaryType
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

    // period predicates
    | expr 'OVERLAPS' expr # PeriodOverlapsPredicate
    | expr 'EQUALS' expr # PeriodEqualsPredicate
    | expr 'CONTAINS' expr # PeriodContainsPredicate
    | expr 'PRECEDES' expr # PeriodPrecedesPredicate
    | expr 'SUCCEEDS' expr # PeriodSucceedsPredicate
    | expr 'IMMEDIATELY' 'PRECEDES' expr # PeriodImmediatelyPrecedesPredicate
    | expr 'IMMEDIATELY' 'SUCCEEDS' expr # PeriodImmediatelySucceedsPredicate

    | expr compOp quantifier quantifiedComparisonPredicatePart3 # QuantifiedComparisonPredicate
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
    | TILDE numericExpr #NumericBitwiseNotExpr
    | numericExpr AMPERSAND numericExpr #NumericBitwiseAndExpr
    | numericExpr (BITWISE_OR | BITWISE_XOR) numericExpr #NumericBitwiseOrExpr
    | numericExpr (BITWISE_SHIFT_LEFT | BITWISE_SHIFT_RIGHT) numericExpr #NumericBitwiseShiftExpr
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
    | generateSeries # GenerateSeriesFunction

    | 'EXISTS' subquery # ExistsPredicate

    | (schemaName '.')? 'HAS_ANY_COLUMN_PRIVILEGE' '('
        ( userString ',' )?
        tableString ','
        privilegeString
      ')' # HasAnyColumnPrivilegePredicate

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

    | (schemaName '.')? 'VERSION' '(' ')' #PostgresVersionFunction

    | (schemaName '.')? 'PG_GET_USERBYID' '(' relOwner=columnReference ')' # PostgresGetUserByIdFunction
    | (schemaName '.')? 'PG_TABLE_IS_VISIBLE' '(' columnOid=columnReference ')' # PostgresTableIsVisibleFunction

    // numeric value functions
    | 'POSITION' '(' expr 'IN' expr ( 'USING' charLengthUnits )? ')' # PositionFunction
    | 'EXTRACT' '(' extractField 'FROM' extractSource ')' # ExtractFunction
    | ('CHAR_LENGTH' | 'CHARACTER_LENGTH') '(' expr ('USING' charLengthUnits)? ')' # CharacterLengthFunction
    | 'OCTET_LENGTH' '(' expr ')' # OctetLengthFunction
    | 'LENGTH' '(' expr ')' # LengthFunction
    | 'CARDINALITY' '(' expr ')' # CardinalityFunction
    | 'ARRAY_UPPER' '(' expr ',' expr ')' # ArrayUpperFunction
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
        ('FROM'? startPosition ( 'FOR'? stringLength )? ( 'USING' charLengthUnits )?
         | ',' startPosition (',' stringLength)? )
      ')' # CharacterSubstringFunction

    | 'UPPER' '(' expr ')' # UpperFunction
    | 'LOWER' '(' expr ')' # LowerFunction
    | 'TRIM' '(' trimSpecification? trimCharacter? 'FROM'? trimSource ')' # TrimFunction
    | 'LOCAL_NAME' '(' expr ')' # LocalNameFunction
    | 'NAMESPACE' '(' expr ')' # NamespaceFunction
    | 'STR' '(' expr (',' expr)* ')' # StrFunction

    | 'OVERLAY' '('
        expr
        'PLACING' expr
        'FROM' startPosition
        ( 'FOR' stringLength )?
        ( 'USING' charLengthUnits )?
      ')' # OverlayFunction

    | 'REPLACE' '(' expr ',' replaceTarget ',' replacement ')' # ReplaceFunction

    | (schemaName '.')? 'CURRENT_USER' # CurrentUserFunction
    | (schemaName '.')? 'CURRENT_SCHEMA' ('(' ')')? # CurrentSchemaFunction
    | (schemaName '.')? 'CURRENT_SCHEMAS' '(' expr ')' # CurrentSchemasFunction
    | (schemaName '.')? 'CURRENT_DATABASE' ('(' ')')? # CurrentDatabaseFunction
    | (schemaName '.')? 'PG_GET_EXPR' ('(' expr ',' expr (',' expr)? ')')? # PgGetExprFunction
    | (schemaName '.')? '_PG_EXPANDARRAY' ('(' expr ')')? # PgExpandArrayFunction
    | (schemaName '.')? 'PG_GET_INDEXDEF' '(' expr (',' expr ',' expr)? ')' # PgGetIndexdefFunction
    | PG_SLEEP '(' sleepSeconds=expr ')' # PgSleepFunction
    | PG_SLEEP_FOR '(' sleepPeriod=expr ')' # PgSleepForFunction

    | currentInstantFunction # CurrentInstantFunction0
    | 'CURRENT_SETTING' '(' expr ')' #CurrentSettingFunction
    | 'CURRENT_TIME' ('(' precision ')')? # CurrentTimeFunction
    | 'LOCALTIME' ('(' precision ')')? # LocalTimeFunction
    | 'DATE_TRUNC' '(' dateTruncPrecision ',' dateTruncSource (',' dateTruncTimeZone)? ')' # DateTruncFunction
    | 'DATE_BIN' '(' intervalLiteral ',' dateBinSource (',' dateBinOrigin)? ')' # DateBinFunction
    | 'RANGE_BINS' '(' intervalLiteral ',' rangeBinsSource (',' dateBinOrigin)? ')' #RangeBinsFunction
    | 'OVERLAPS' '(' expr ( ',' expr )+ ')' # OverlapsFunction
    | ('PERIOD' | 'TSTZRANGE') '(' expr ',' expr ')' # TsTzRangeConstructor
    | 'UPPER_INF' '(' expr ')' # UpperInfFunction
    | 'LOWER_INF' '(' expr ')' # LowerInfFunction

    // interval value functions
    | 'AGE' '(' expr ',' expr ')' # AgeFunction

    | 'TRIM_ARRAY' '(' expr ',' expr ')' # TrimArrayFunction
    ;

replaceTarget : expr;
replacement : expr;

currentInstantFunction
    : 'CURRENT_DATE' ( '(' ')' )? # CurrentDateFunction
    | ('CURRENT_TIMESTAMP' | 'NOW') ('(' precision? ')')? # CurrentTimestampFunction
    | 'LOCALTIMESTAMP' ('(' precision ')')? # LocalTimestampFunction
    ;

booleanValue : 'TRUE' | 'FALSE' | 'UNKNOWN' ;

// spec addition: objectConstructor

objectConstructor
    : ('RECORD' | 'OBJECT') '(' (objectNameAndValue (',' objectNameAndValue)*)? ')'
    | '{' (objectNameAndValue (',' objectNameAndValue)*)? '}'
    ;

objectNameAndValue : objectName ':' expr ;

objectName : identifier ;

parameterSpecification
    : '?' #DynamicParameter
    | POSTGRES_PARAMETER_SPECIFICATION #PostgresParameter
    ;

staticExpr
    : literal #StaticLiteral
    | parameterSpecification #StaticParam
    ;

columnReference : identifierChain ;

/// generate_series function

generateSeries : (schemaName '.')? 'GENERATE_SERIES' '(' seriesStart ',' seriesEnd (',' seriesStep)? ')' ;
seriesStart: expr;
seriesEnd: expr;
seriesStep: expr;

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

dateBinSource : expr ;
rangeBinsSource : expr ;
dateBinOrigin : expr ;

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

recordValueConstructor
    : parameterSpecification # ParameterRecord
    | objectConstructor # ObjectRecord
    ;

recordsValueList : recordValueConstructor (',' recordValueConstructor)* ;

recordsValueConstructor : 'RECORDS' recordsValueList ;

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
    | generateSeries withOrdinality? tableAlias tableProjection? # GenerateSeriesTable
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
    | ('NOW' | 'CURRENT_TIMESTAMP') #PeriodSpecNow
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

whereClause : 'WHERE' searchCondition ;

/// §7.9 <group by clause>

groupByClause : 'GROUP' 'BY' (setQuantifier)? groupingElement (',' groupingElement)* ;

groupingElement
    : columnReference # OrdinaryGroupingSet
    | '(' ')' # EmptyGroupingSet
    ;

/// §7.10 <having clause>

havingClause : 'HAVING' searchCondition ;

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
selectList
    : selectListAsterisk (',' selectSublist?)*
    | selectSublist? (',' selectSublist?)* (',' selectListAsterisk)?
    ;

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

queryExpression : withClause? queryExpressionNoWith ;
queryExpressionNoWith : queryExpressionBody orderByClause? offsetAndLimit?  ;
withClause : 'WITH' RECURSIVE? withListElement (',' withListElement)* ;
withListElement : queryName ('(' columnNameList ')')? 'AS' subquery ;

queryExpressionBody
    : queryTerm # QueryBodyTerm
    | queryExpressionBody 'UNION' (ALL | DISTINCT)? queryTerm # UnionQuery
    | queryExpressionBody 'EXCEPT' (ALL | DISTINCT)? queryTerm # ExceptQuery
    ;

queryTerm
    : selectClause fromClause? whereClause? groupByClause? havingClause? windowClause? # QuerySpecification
    | fromClause queryTail* windowClause? # QuerySpecification
    | tableValueConstructor # ValuesQuery
    | recordsValueConstructor # RecordsQuery
    | '(' queryExpressionNoWith ')' # WrappedQuery
    | queryTerm 'INTERSECT' (ALL | DISTINCT)? queryTerm # IntersectQuery
    ;

queryTail
    : whereClause (groupByClause? havingClause? selectClause)?
    | groupByClause? havingClause? selectClause
    ;

orderByClause : 'ORDER' 'BY' sortSpecificationList ;

offsetAndLimit
    : resultOffsetClause fetchFirstClause?
    | fetchFirstClause resultOffsetClause?
    ;

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
    | compOp quantifier quantifiedComparisonPredicatePart3 # QuantifiedComparisonPredicatePart2
    ;

quantifiedComparisonPredicatePart3
  : subquery # QuantifiedComparisonSubquery
  | expr #QuantifiedComparisonExpr
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

/// §8.21 <search condition>

searchCondition : expr? (',' expr?)* ;

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
    // Removed 'ANY' and 'SOME' as aggregate functions, as it introduces ambiguities with
    // the `= ANY` comparison operator.  (Following the same approach as PostgreSQL).
    | 'EVERY' | 'BOOL_AND' | 'BOOL_OR'
    | 'STDDEV_POP' | 'STDDEV_SAMP' | 'VAR_SAMP' | 'VAR_POP' ;

setQuantifier : 'DISTINCT' | 'ALL' ;

/// §10.10 <sort specification list>

sortSpecificationList : sortSpecification (',' sortSpecification)* ;
sortSpecification : expr orderingSpecification? nullOrdering? ;
orderingSpecification : 'ASC' | 'DESC' ;
nullOrdering : 'NULLS' 'FIRST' | 'NULLS' 'LAST' ;

/// §14 Data manipulation

/// Postgres return statements
returningStatement : 'RETURNING' selectList # DmlReturningStatement;

/// §14.9 <delete statement: searched>

deleteStatementSearched
    : 'DELETE' 'FROM' tableName
      dmlStatementValidTimeExtents?
      ( 'AS'? correlationName )?
      ( 'WHERE' searchCondition )?
      returningStatement?
    ;

dmlStatementValidTimeExtents
  : 'FOR' ('PORTION' 'OF')? 'VALID_TIME' 'FROM' from=staticExpr ('TO' to=staticExpr)? # DmlStatementValidTimePortion
  | 'FOR' ('ALL' 'VALID_TIME' | 'VALID_TIME' 'ALL') # DmlStatementValidTimeAll
  ;

patchStatement
  : PATCH INTO tableName
    ('FOR' ('PORTION' 'OF')? 'VALID_TIME' 'FROM' validFrom=staticExpr ('TO' validTo=staticExpr)?)?
    patchSource
  ;

patchSource
  : recordsValueConstructor # PatchRecords
//  | ( '(' columnNameList ')' )? tableValueConstructor # PatchValues
  ;

eraseStatementSearched : 'ERASE' 'FROM' tableName ( 'AS'? correlationName )? ('WHERE' searchCondition)? ;

/// §14.11 <insert statement>

insertStatement : 'INSERT' 'INTO' tableName insertColumnsAndSource returningStatement?;
insertColumnsAndSource
    : ( '(' columnNameList ')' )? tableValueConstructor # InsertValues
    | ( '(' columnNameList ')' )? recordsValueConstructor # InsertRecords
    | ( '(' columnNameList ')' )? queryExpression # InsertFromSubquery
    ;

/// §14.14 <update statement: searched>

updateStatementSearched
    : 'UPDATE' tableName
      dmlStatementValidTimeExtents?
      ( 'AS'? correlationName )?
      'SET' setClauseList
      ( 'WHERE' searchCondition )?
      returningStatement?
    ;

/// §14.15 <set clause list>

setClauseList : setClause (',' setClause)* ;
setClause : setTarget '=' updateSource ;

// TODO SQL:2011 supports updating keys within a struct here
setTarget : columnName ;

updateSource : expr ;

/// §17.3 <transaction characteristics>

sessionCharacteristic
    : 'TRANSACTION' sessionTxMode (',' sessionTxMode)* # SessionTxCharacteristics
    ;

sessionTxMode
    : 'ISOLATION' 'LEVEL' levelOfIsolation  # SessionIsolationLevel
    | 'READ' 'ONLY' # ReadOnlySession
    | 'READ' 'WRITE' # ReadWriteSession
    ;

transactionCharacteristics : transactionMode (',' transactionMode)* ;

transactionMode
    : 'ISOLATION' 'LEVEL' levelOfIsolation  # IsolationLevel
    | 'READ' 'ONLY' # ReadOnlyTransaction
    | 'READ' 'WRITE' # ReadWriteTransaction
    | 'AT' 'SYSTEM_TIME' dateTimeLiteral #TransactionSystemTime
    ;

levelOfIsolation
    : 'READ' 'UNCOMMITTED' # ReadUncommittedIsolation
    | 'READ' 'COMMITTED' # ReadCommittedIsolation
    | 'REPEATABLE' 'READ' # RepeatableReadIsolation
    | 'SERIALIZABLE' # SerializableIsolation
    ;
