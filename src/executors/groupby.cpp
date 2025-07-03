#include "global.h"
BinaryOperator getBinaryOperator(const string& op) {
    if (op == "<") return LESS_THAN;
    if (op == ">") return GREATER_THAN;
    if (op == "<=") return LEQ;
    if (op == ">=") return GEQ;
    if (op == "==") return EQUAL;
    if (op == "!=") return NOT_EQUAL;
    return NO_BINOP_CLAUSE;  
}
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");

    if (tokenizedQuery.size() != 13 || tokenizedQuery[1] != "<-")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupbyResultRelationName = tokenizedQuery[0];
    parsedQuery.groupbyColumnName = tokenizedQuery[4];
    parsedQuery.groupbyRelationName = tokenizedQuery[6];

    string havingClause1 = tokenizedQuery[8]; 
    size_t openParen1 = havingClause1.find('(');
    size_t closeParen1 = havingClause1.find(')');

    if (openParen1 != string::npos && closeParen1 != string::npos) {
        parsedQuery.groupbyAggregateFunction = havingClause1.substr(0, openParen1);
        parsedQuery.groupbyAggregateColumn = havingClause1.substr(openParen1 + 1, closeParen1 - openParen1 - 1);
    }

    
    parsedQuery.groupbyBinaryOperator = getBinaryOperator(tokenizedQuery[9]);

    
    try {
        parsedQuery.groupbyattribute_value = stoi(tokenizedQuery[10]);  
    } catch (invalid_argument& e) {
        cout << "SYNTAX ERROR: Invalid numeric value in GROUP BY condition" << endl;
        return false;
    }

    string havingClause2 = tokenizedQuery[12]; 
    size_t openParen2 = havingClause2.find('(');
    size_t closeParen2 = havingClause2.find(')');

    if (openParen2 != string::npos && closeParen2 != string::npos) {
        parsedQuery.groupbyAggregateFunction2 = havingClause2.substr(0, openParen2);
        parsedQuery.groupbyAggregateColumn2 = havingClause2.substr(openParen2 + 1, closeParen2 - openParen2 - 1);
    }

    return true;
}


bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupbyResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Relation '" << parsedQuery.groupbyRelationName << "' doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyColumnName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Group By column '" << parsedQuery.groupbyColumnName << "' doesn't exist in relation '"
             << parsedQuery.groupbyRelationName << "'" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyAggregateColumn, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Aggregate column '" << parsedQuery.groupbyAggregateColumn << "' doesn't exist in relation '"
             << parsedQuery.groupbyRelationName << "'" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyAggregateColumn2, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Aggregate column 2 '" << parsedQuery.groupbyAggregateColumn2 << "' doesn't exist in relation '"
             << parsedQuery.groupbyRelationName << "'" << endl;
        return false;
    }

    return true;
}

int computeAggregate(const string &func, const vector<int> &values)
{
    if (func == "AVG")
    {
        int sum = accumulate(values.begin(), values.end(), 0);
        return (values.empty() ? 0 : sum / values.size());
    }
    else if (func == "SUM")
    {
        return accumulate(values.begin(), values.end(), 0);
    }
    else if (func == "MAX")
    {
        return *max_element(values.begin(), values.end());
    }
    else if (func == "MIN")
    {
        return *min_element(values.begin(), values.end());
    }
    else if (func == "COUNT")
    {
        return values.size();
    }
    return 0;
}

bool evaluateCondition(int aggValue, BinaryOperator op, int threshold)
{
    switch (op)
    {
    case GREATER_THAN: return aggValue > threshold;
    case LESS_THAN:    return aggValue < threshold;
    case GEQ:          return aggValue >= threshold;
    case LEQ:          return aggValue <= threshold;
    case EQUAL:        return aggValue == threshold;
    case NOT_EQUAL:    return aggValue != threshold;
    default:           return false;
    }
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    Table *inputTable = tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    if (!inputTable)
    {
        cout << "Error: Input table not found!" << endl;
        return;
    }

    vector<string> resultColumns = {parsedQuery.groupbyColumnName,
                                    parsedQuery.groupbyAggregateFunction2 + parsedQuery.groupbyAggregateColumn2};

    Table *resultTable = new Table(parsedQuery.groupbyResultRelationName, resultColumns);
    if (!resultTable)
    {
        cout << "Error: Failed to create result table!" << endl;
        return;
    }

    parsedQuery.sortRelationName = parsedQuery.groupbyRelationName;
    parsedQuery.sortColumns.clear();
    parsedQuery.sortColumns.push_back(parsedQuery.groupbyColumnName);
    parsedQuery.sortingStrategies.clear();
    parsedQuery.sortingStrategies.push_back(ASC);

    executeSORT();

    Cursor cursor = inputTable->getCursor();

    bool firstRow = true;
    int currentGroup = 0;
    vector<int> groupHavingValues;
    vector<int> groupReturnValues;
    bool done = false;

    vector<vector<int>> resultBuffer;
    int countrows = 0;

    while (!done)
    {
        vector<vector<int>> block = cursor.getNextBlock();

        if (block.empty())
        {
            done = true;
            break;
        }

        for (const auto &row : block)
        {
            if (row.empty())
            {
                continue;
            }

            if (row.size() <= inputTable->getColumnIndex(parsedQuery.groupbyColumnName) ||
                row.size() <= inputTable->getColumnIndex(parsedQuery.groupbyAggregateColumn) ||
                row.size() <= inputTable->getColumnIndex(parsedQuery.groupbyAggregateColumn2))
            {
                cout << "Error: Row size mismatch. Skipping this row." << endl;
                continue;
            }

            int groupValue = row[inputTable->getColumnIndex(parsedQuery.groupbyColumnName)];
            int havingVal = row[inputTable->getColumnIndex(parsedQuery.groupbyAggregateColumn)];
            int returnVal = row[inputTable->getColumnIndex(parsedQuery.groupbyAggregateColumn2)];

            if (firstRow)
            {
                currentGroup = groupValue;
                firstRow = false;
            }

            if (groupValue == currentGroup)
            {
                groupHavingValues.push_back(havingVal);
                groupReturnValues.push_back(returnVal);
            }
            else
            {
                int aggValue1 = computeAggregate(parsedQuery.groupbyAggregateFunction, groupHavingValues);
                
                if (evaluateCondition(aggValue1, parsedQuery.groupbyBinaryOperator, parsedQuery.groupbyattribute_value))
                {
                    int aggValue2 = computeAggregate(parsedQuery.groupbyAggregateFunction2, groupReturnValues);
                    
                    resultBuffer.push_back({currentGroup, aggValue2});
                    
                    if (resultBuffer.size() >= 9 * resultTable->maxRowsPerBlock)
                    {
                        for (const auto &resultRow : resultBuffer)
                        {
                            countrows++;
                            resultTable->writeRow(resultRow);
                        }
                        resultBuffer.clear();
                    }
                }

                currentGroup = groupValue;
                groupHavingValues.clear();
                groupReturnValues.clear();
                groupHavingValues.push_back(havingVal);
                groupReturnValues.push_back(returnVal);
            }
        }
    }

    if (!groupHavingValues.empty())
    {
        int aggValue1 = computeAggregate(parsedQuery.groupbyAggregateFunction, groupHavingValues);
        if (evaluateCondition(aggValue1, parsedQuery.groupbyBinaryOperator, parsedQuery.groupbyattribute_value))
        {
            int aggValue2 = computeAggregate(parsedQuery.groupbyAggregateFunction2, groupReturnValues);
     
            resultBuffer.push_back({currentGroup, aggValue2});
        }
    }

    if (!resultBuffer.empty())
    {
        for (const auto &resultRow : resultBuffer)
        {
            countrows++;
            resultTable->writeRow(resultRow);
        }
        resultBuffer.clear();
    }

    if (countrows == 0)
    {
        string sourceFileName = "../data/temp/" + resultTable->tableName + ".csv";
        bufferManager.deleteFile(sourceFileName);
        return;
    }

    resultTable->blockify();
    tableCatalogue.insertTable(resultTable);
}