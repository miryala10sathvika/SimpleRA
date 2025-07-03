#include "global.h"

/**
 * @brief 
 * SYNTAX: R <- DISTINCT relation_name
 */
bool syntacticParseCHECKANTISYM()
{
    logger.log("syntacticParseCHECKANTISYM");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CHECKANTISYM;
    parsedQuery.checkAntiSymRelation1 = tokenizedQuery[1];
    parsedQuery.checkAntiSymRelation2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCHECKANTISYM()
{
    logger.log("semanticParseCHECKANTISYM");
    //The resultant table shouldn't exist and the table argument should
    if (!matrixCatalogue.isMatrix(parsedQuery.checkAntiSymRelation1))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }

    if (!matrixCatalogue.isMatrix(parsedQuery.checkAntiSymRelation2))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }
    return true;
}

void executeCHECKANTISYM()
{
    logger.log("executeCHECKANTISYM");
    Matrix* matrix1 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymRelation1);
    Matrix* matrix2 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymRelation2);
    bool result=matrix2->CheckAntiSym(matrix1->matrixName);
    if(result==0)
    cout<<"False"<<endl;
    else 
    cout<<"True"<<endl;
    return;
}