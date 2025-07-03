#include "global.h"

bool syntacticParse()
{
    logger.log("syntacticParse");
    string possibleQueryType = tokenizedQuery[0];

    if (tokenizedQuery.size() < 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (possibleQueryType == "CLEAR")
        return syntacticParseCLEAR();
    else if (possibleQueryType == "INDEX")
        return syntacticParseINDEX();
    else if (possibleQueryType == "LIST")
        return syntacticParseLIST();
    else if (possibleQueryType == "LOAD" && tokenizedQuery[1] == "MATRIX")
        return syntacticParseLOADMATRIX();
    else if (possibleQueryType == "PRINT" && tokenizedQuery[1] == "MATRIX")
        return syntacticParsePRINTMATRIX();
    else if (possibleQueryType == "EXPORT" && tokenizedQuery[1] == "MATRIX")
        return syntacticParseEXPORTMATRIX();
    else if(possibleQueryType=="CROSSTRANSPOSE")
        return syntacticParseCROSSTRANSPOSE();
    else if(possibleQueryType=="ROTATE")
        return syntacticParseROTATE();
    else if(possibleQueryType=="CHECKANTISYM")
        return syntacticParseCHECKANTISYM();
    else if (possibleQueryType == "LOAD")
        return syntacticParseLOAD();
    else if (possibleQueryType == "PRINT")
        return syntacticParsePRINT();
    else if (possibleQueryType == "RENAME")
        return syntacticParseRENAME();
    else if(possibleQueryType == "EXPORT")
        return syntacticParseEXPORT();
    else if(possibleQueryType == "SOURCE")
        return syntacticParseSOURCE();
    else if(possibleQueryType == "DELETE")
        return syntacticParseDELETE();
    else if(possibleQueryType == "INSERT")
        return syntacticParseINSERT();
    else if(possibleQueryType == "UPDATE")
        return syntacticParseUPDATE();
    else
    {
        string resultantRelationName = possibleQueryType;
        if (tokenizedQuery[0]!="SORT" && (tokenizedQuery[1] != "<-" || tokenizedQuery.size() < 3))
        {
            // cout <<"j"<<endl;
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        possibleQueryType = tokenizedQuery[2];
        if(possibleQueryType=="SEARCH")
        return syntacticParseSEARCH();
        if (possibleQueryType == "PROJECT")
            return syntacticParsePROJECTION();
        else if (possibleQueryType == "SELECT")
            return syntacticParseSELECTION();
        else if (possibleQueryType == "JOIN")
            return syntacticParseJOIN();
        else if (possibleQueryType == "CROSS")
            return syntacticParseCROSS();
        else if (possibleQueryType == "DISTINCT")
            return syntacticParseDISTINCT();
        else if (tokenizedQuery[0]=="SORT")
        {
            possibleQueryType = "SORT";
            return syntacticParseSORT();
        }
        else if(possibleQueryType=="ORDER" && tokenizedQuery[3]=="BY")
        {
            return syntacticParseORDERBY();
        }
        else if(possibleQueryType=="GROUP" && tokenizedQuery[3]=="BY")
        {
            return syntacticParseGROUPBY();
        }
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    return false;
}

ParsedQuery::ParsedQuery()
{
}

void ParsedQuery::clear()
{
    logger.log("ParseQuery::clear");
    this->queryType = UNDETERMINED;

    this->clearRelationName = "";

    this->crossResultRelationName = "";
    this->crossFirstRelationName = "";
    this->crossSecondRelationName = "";

    this->distinctResultRelationName = "";
    this->distinctRelationName = "";
    this->orderbyColumnName="";
    this->orderbySortingStrategy="";

    this->orderbyResultRelationName = "";
    this->orderbyRelationName = "";

    this->exportRelationName = "";

    this->indexingStrategy = NOTHING;
    this->indexColumnName = "";
    this->indexRelationName = "";

    this->joinBinaryOperator = NO_BINOP_CLAUSE;
    this->joinResultRelationName = "";
    this->joinFirstRelationName = "";
    this->joinSecondRelationName = "";
    this->joinFirstColumnName = "";
    this->joinSecondColumnName = "";

    this->loadRelationName = "";

    this->printRelationName = "";

    this->projectionResultRelationName = "";
    this->projectionColumnList.clear();
    this->projectionRelationName = "";

    this->renameFromColumnName = "";
    this->renameToColumnName = "";
    this->renameRelationName = "";
    this->insertrelationName = "";
    this->insertColumns .clear();
    this->insertValues.clear();
    this->updateconditionColumn = "";
    this->updateRelation = "";
    this->updateconditionValue = 0;
    this->updateBinaryOperator = NO_BINOP_CLAUSE;
    this->updateColumn = "";
    this->updateValue = 0;
    this->selectType = NO_SELECT_CLAUSE;
    this->selectionBinaryOperator = NO_BINOP_CLAUSE;
    this->selectionResultRelationName = "";
    this->selectionRelationName = "";
    this->selectionFirstColumnName = "";
    this->selectionSecondColumnName = "";
    this->selectionIntLiteral = 0;
    this->groupbyColumnName = "";
    this->groupbyRelationName = "";
    this->groupbyResultRelationName = "";
    this->groupbyAggregateFunction = "";
    this->groupbyAggregateColumn = "";
    this->groupbyattribute_value = 0;
    this->groupbyBinaryOperator = NO_BINOP_CLAUSE;
    this->groupbyAggregateFunction2 = "";
    this->groupbyAggregateColumn2 = ""; 
    this->sortingStrategy = NO_SORT_CLAUSE;
    this->sortResultRelationName = "";
    this->sortColumnName = "";
    this->sortRelationName = "";

    this->sourceFileName = "";

    this->checkAntiSymRelation1="";
    this->checkAntiSymRelation2="";

    this->rotateRelationName="";
}

/**
 * @brief Checks to see if source file exists. Called when LOAD command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isFileExists(string tableName)
{
    string fileName = "../data/" + tableName + ".csv";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

/**
 * @brief Checks to see if source file exists. Called when SOURCE command is
 * invoked.
 *
 * @param tableName 
 * @return true 
 * @return false 
 */
bool isQueryFile(string fileName){
    fileName = "../data/" + fileName + ".ra";
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}