#include "global.h"

void checkforDeletion()
{
    string tableName1 = "";
    string tableName2 = "";

    if (parsedQuery.queryType == EXPORT)
    {
        tableName1 = parsedQuery.exportRelationName;
    }
    if (parsedQuery.queryType == INDEX)
    {
        tableName1 = parsedQuery.indexColumnName;
    }
    if (parsedQuery.queryType == JOIN)
    {
        tableName1 = parsedQuery.joinFirstRelationName;
        tableName2 = parsedQuery.joinSecondRelationName;
    }
    if (parsedQuery.queryType == CROSS)
    {
        tableName1 = parsedQuery.crossFirstRelationName;
        tableName2 = parsedQuery.crossSecondRelationName;
    }
    if (parsedQuery.queryType == PRINT)
    {
        tableName1 = parsedQuery.printRelationName;
    }
    if (parsedQuery.queryType == PROJECTION)
    {
        tableName1 = parsedQuery.projectionRelationName;
    }
    if (parsedQuery.queryType == SELECTION)
    {
        tableName1 = parsedQuery.selectionRelationName;
    }
    if (parsedQuery.queryType == SORT)
    {
        tableName1 = parsedQuery.sortRelationName;
    }
    if (parsedQuery.queryType == ORDERBY)
    {
        tableName1 = parsedQuery.orderbyRelationName;
    }
    if (parsedQuery.queryType == GROUPBY)
    {
        tableName1 = parsedQuery.groupbyRelationName;
    }
    if (parsedQuery.queryType == SEARCH)
    {
        tableName1 = parsedQuery.searchRelationName;
    }
    if (parsedQuery.queryType == DELETE)
    {
        tableName1 = parsedQuery.deleteRelationName;
    }

    if (tableName1 != "" && parsedQuery.queryType != DELETE)
    {
        Table *table = tableCatalogue.getTable(tableName1);
        table->rowsPerBlockCount.resize(table->blockCount);
        if (table->deleteCount > 0)
        {
            deleteRowsFromTable_Blocks(table);
            deleteRowsFromTable_Threshold(table);

            for (int p0 = 0; p0 < table->columnCount; p0++)
            {
                string L1Name = table->tableName + table->columns[p0] + "L1";
                if (tableCatalogue.isTable(L1Name))
                {
                    Table *l1table = tableCatalogue.getTable(L1Name);
                    l1table->dirtyIndex = true;
                }
            }
        }

        // change made
        if (parsedQuery.queryType == SORT || parsedQuery.queryType == GROUPBY)
        {
            for (int p0 = 0; p0 < table->columnCount; p0++)
            {
                string L1Name = table->tableName + table->columns[p0] + "L1";
                string L2Name = table->tableName + table->columns[p0] + "L2";
                if (tableCatalogue.isTable(L1Name))
                    tableCatalogue.deleteTable(L1Name);
                if (tableCatalogue.isTable(L2Name))
                    tableCatalogue.deleteTable(L2Name);
            }
        }
    }
    if (tableName2 != "" && parsedQuery.queryType != DELETE)
    {
        Table *table1 = tableCatalogue.getTable(tableName2);
        table1->rowsPerBlockCount.resize(table1->blockCount);
        if (table1->deleteCount > 0)
        {
            deleteRowsFromTable_Blocks(table1);
            deleteRowsFromTable_Threshold(table1);
            for (int p0 = 0; p0 < table1->columnCount; p0++)
            {
                string L1Name = table1->tableName + table1->columns[p0] + "L1";
                if (tableCatalogue.isTable(L1Name))
                {
                    Table *l1table = tableCatalogue.getTable(L1Name);
                    l1table->dirtyIndex = true;
                }
            }
        }
    }

    if (parsedQuery.queryType == SEARCH || parsedQuery.queryType == DELETE || parsedQuery.queryType == UPDATE)
    {

        Table *table2 = tableCatalogue.getTable(tableName1);
        if (parsedQuery.queryType == SEARCH)
        {
            Table *temp = tableCatalogue.getTable(tableName1 + parsedQuery.searchFirstColumnName + "L1");
            if (temp && temp->dirtyIndex)
            {
                buildIndex(table2, parsedQuery.searchFirstColumnName);
                temp->dirtyIndex = false;
            }
        }
        else if (parsedQuery.queryType == DELETE && table2->dirtyIndex == true)
        {
            Table *temp = tableCatalogue.getTable(tableName1 + parsedQuery.deleteColumnName + "L1");
            if (temp && temp->dirtyIndex)
            {
                buildIndex(table2, parsedQuery.deleteColumnName);
                temp->dirtyIndex = false;
            }
        }
        table2 = tableCatalogue.getTable(parsedQuery.updateRelation);
        if (parsedQuery.queryType == UPDATE)
        {
            Table *temp = tableCatalogue.getTable(tableName1 + parsedQuery.updateconditionColumn + "L1");
            if (temp && temp->dirtyIndex)
            {
                buildIndex(table2, parsedQuery.updateconditionColumn);
                temp->dirtyIndex = false;
            }
        }
    }
}
bool semanticParse()
{
    logger.log("semanticParse");
    switch (parsedQuery.queryType)
    {
    case CLEAR:
    {
        return semanticParseCLEAR();
    }
    case CROSS:
    {
        bool val = semanticParseCROSS();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseCROSS();
    }
    case DISTINCT:
    {
        return semanticParseDISTINCT();
    }
    case EXPORT:
    {
        bool val = semanticParseEXPORT();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseEXPORT();
    }
    case INDEX:
    {
        bool val = semanticParseINDEX();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseINDEX();
    }
    case JOIN:
    {
        bool val = semanticParseJOIN();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseJOIN();
    }
    case LIST:
    {
        return semanticParseLIST();
    }
    case LOAD:
    {
        return semanticParseLOAD();
    }
    case LOADMATRIX:
    {
        return semanticParseLOADMATRIX();
    }
    case PRINTMATRIX:
    {
        return semanticParsePRINTMATRIX();
    }
    case PRINT:
    {
        bool val = semanticParsePRINT();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParsePRINT();
    }
    case PROJECTION:
    {
        bool val = semanticParsePROJECTION();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParsePROJECTION();
    }
    case RENAME:
    {
        return semanticParseRENAME();
    }
    case SELECTION:
    {
        bool val = semanticParseSELECTION();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseSELECTION();
    }
    case SORT:
    {
        bool val = semanticParseSORT();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseSORT();
    }
    case SOURCE:
    {
        return semanticParseSOURCE();
    }
    case EXPORTMATRIX:
    {
        return semanticParseEXPORTMATRIX();
    }

    case ROTATE:
    {
        return semanticParseROTATE();
    }
    case CROSSTRANSPOSE:
    {
        return semanticParseCROSSTRANSPOSE();
    }
    case CHECKANTISYM:
    {
        return semanticParseCHECKANTISYM();
    }
    case ORDERBY:
    {
        bool val = semanticParseORDERBY();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseORDERBY();
    }
    case GROUPBY:
    {
        bool val = semanticParseGROUPBY();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseGROUPBY();
    }
    case SEARCH:
    {
        bool val = semanticParseSEARCH();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseSEARCH();
    }
    case INSERT:
    {
        bool val = semanticParseINSERT();
        if (!val)
            return val;
        return semanticParseINSERT();
    }
    case UPDATE:
    {
        bool val = semanticParseUPDATE();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseUPDATE();
    }
    case DELETE:
    {
        bool val = semanticParseDELETE();
        if (!val)
            return val;
        checkforDeletion();
        return semanticParseDELETE();
    }

    default:
        cout << "SEMANTIC ERROR" << endl;
    }

    return false;
}