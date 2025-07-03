#include "tableCatalogue.h"
#include "matrixCatalogue.h" 

using namespace std;

enum QueryType
{
    CLEAR,
    CROSS,
    DISTINCT,
    EXPORT,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    UNDETERMINED,
    LOADMATRIX,
    EXPORTMATRIX,
    CROSSTRANSPOSE,
    ROTATE,
    PRINTMATRIX,
    CHECKANTISYM,
    ORDERBY,
    GROUPBY,
    SEARCH,
    DELETE,
    INSERT,
    UPDATE,
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string orderbyResultRelationName = "";
    string orderbyRelationName = "";
    string orderbySortingStrategy="";
    string orderbyColumnName = "";

    string exportRelationName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";
    string insertrelationName = "";
    vector<string> insertColumns = {};
    vector<int> insertValues = {};
    string updateconditionColumn = "";
    string updateRelation = "";
    int updateconditionValue = 0 ;
    BinaryOperator updateBinaryOperator = NO_BINOP_CLAUSE;
    string updateColumn = "";
    int updateValue = 0;
    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";

    string searchResultRelationName = "";
    string searchRelationName = "";
    string searchFirstColumnName = "";
    BinaryOperator searchBinaryOperator = NO_BINOP_CLAUSE;

    BinaryOperator deleteBinaryOperator = NO_BINOP_CLAUSE;
    string deleteRelationName="";
    string deleteColumnName="";
    int deleteIntLiteral=0;

    string groupbyColumnName = "";
    string groupbyRelationName = "";
    string groupbyResultRelationName = "";
    string groupbyAggregateFunction = "";
    string groupbyAggregateColumn = "";
    BinaryOperator groupbyBinaryOperator = NO_BINOP_CLAUSE;
    int groupbyattribute_value = 0;
    string groupbyAggregateFunction2 = "";
    string groupbyAggregateColumn2 = ""; 
    int selectionIntLiteral = 0;
    int searchIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    vector<string> sortColumns = {};
    vector<SortingStrategy> sortingStrategies = {};

    string sourceFileName = "";

    ParsedQuery();
    void clear();

    string checkAntiSymRelation1="";
    string checkAntiSymRelation2="";

    string rotateRelationName="";

};

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINTMATRIX();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseROTATE();
bool syntacticParseCROSSTRANSPOSE();
bool syntacticParseCHECKANTISYM();
bool syntacticParseORDERBY();
bool syntacticParseGROUPBY();
bool syntacticParseSEARCH();
bool syntacticParseDELETE();
bool syntacticParseINSERT();
bool syntacticParseUPDATE();
bool isFileExists(string tableName);
bool isQueryFile(string fileName);