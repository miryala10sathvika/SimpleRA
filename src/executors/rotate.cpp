#include "global.h"

/**
 * @brief 
 * SYNTAX: R <- DISTINCT relation_name
 */
bool syntacticParseROTATE()
{
    logger.log("syntacticParseROTATE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ROTATE;
    parsedQuery.rotateRelationName = tokenizedQuery[1];
    // parsedQuery.checkAntiSymRelation2 = tokenizedQuery[2];
    return true;
}

bool semanticParseROTATE()
{
    logger.log("semanticParseROTATE");
    //The resultant table shouldn't exist and the table argument should
    if (!matrixCatalogue.isMatrix(parsedQuery.rotateRelationName))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }
    return true;
}

void executeROTATE()
{
    logger.log("executeROTATE");
    // cout<<"HERE"<<endl;
    Matrix* matrix1 = matrixCatalogue.getMatrix(parsedQuery.rotateRelationName);
    // cout<<"HERE"<<endl;
    matrix1->RotateMatrix(parsedQuery.rotateRelationName);
    return;
}