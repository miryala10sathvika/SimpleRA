#include "global.h"

// /**
//  * @brief
//  * SYNTAX: R <- DISTINCT relation_name
//  */

bool syntacticParseORDERBY()
{
    logger.log("syntacticParseORDERBY");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[1] != "<-" || tokenizedQuery[6] != "ON")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ORDERBY;
    parsedQuery.orderbyResultRelationName = tokenizedQuery[0];
    parsedQuery.orderbyColumnName = tokenizedQuery[4];
    string orderType = tokenizedQuery[5];
    if (orderType == "ASC")
        parsedQuery.orderbySortingStrategy = "ASC";
    else if (orderType == "DESC")
        parsedQuery.orderbySortingStrategy = "DESC";
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.orderbyRelationName = tokenizedQuery[7];

    return true;
}

bool semanticParseORDERBY()
{
    logger.log("semanticParseORDERBY");
    if (tableCatalogue.isTable(parsedQuery.orderbyResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.orderbyRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    else
    {
        if (!tableCatalogue.isColumnFromTable(parsedQuery.orderbyColumnName, parsedQuery.orderbyRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation";
            return false;
        }
    }
    return true;
}

void executeORDERBY()
{
    logger.log("executeORDERBY");
    Table table = *tableCatalogue.getTable(parsedQuery.orderbyRelationName);
    Table *resultantTable = new Table(parsedQuery.orderbyResultRelationName, table.columns);

    Cursor cursor1 = table.getCursor();
    vector<int> columnIndices;
    for (int columnCounter = 0; columnCounter < table.columns.size(); columnCounter++)
    {
        columnIndices.emplace_back(table.getColumnIndex(table.columns[columnCounter]));
    }
    vector<int> row = cursor1.getNext();
    vector<int> resultantRow(columnIndices.size(), 0);

    while (!row.empty())
    {

        for (int columnCounter = 0; columnCounter < columnIndices.size(); columnCounter++)
        {
            resultantRow[columnCounter] = row[columnIndices[columnCounter]];
        }
        resultantTable->writeRow<int>(resultantRow);
        row = cursor1.getNext();
    }
    resultantTable->blockify();
    tableCatalogue.insertTable(resultantTable);

    parsedQuery.sortRelationName=resultantTable->tableName;
    parsedQuery.sortColumns.clear();
    parsedQuery.sortColumns.push_back(parsedQuery.orderbyColumnName);
    parsedQuery.sortingStrategies.clear();
    if (parsedQuery.orderbySortingStrategy == "ASC")
            parsedQuery.sortingStrategies.push_back(ASC);
        else if (parsedQuery.orderbySortingStrategy == "DESC")
            parsedQuery.sortingStrategies.push_back(DESC);
    
    executeSORT();
    return;
}