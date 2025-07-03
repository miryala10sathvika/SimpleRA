#include "global.h"
/**
 * @brief
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */


bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[5] != "ON" || tokenizedQuery[1] != "<-")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];

    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[7];

    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

uint32_t simpleHash(int row, uint32_t numPartitions)
{
    return (row * 2654435761u) % numPartitions; 
}

void executeJOIN()
{
    logger.log("executeJOIN");
    int k = 10;
    vector<vector<vector<int>>> buffers(k);
    Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Cursor cursor = table1->getCursor();
    int hashElem1 = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int pagetoWriteto = table1->blockCount;
    unordered_map<int, vector<int>> pageMapper;
    for (int i = 0; i < table1->blockCount; i++)
    {
        cursor.nextPage(i);
        buffers[k - 1] = cursor.getNextBlock();
        for (int j = 0; j < buffers[k - 1].size(); j++)
        {
            int bucket = simpleHash(buffers[k - 1][j][hashElem1], k - 1);
            buffers[bucket].push_back(buffers[k - 1][j]);
            if (buffers[bucket].size() == table1->maxRowsPerBlock)
            {
                bufferManager.writePage(parsedQuery.joinFirstRelationName, pagetoWriteto, buffers[bucket], buffers[bucket].size(), 6, 0, "", buffers[bucket]);
                pageMapper[bucket].push_back(pagetoWriteto);
                table1->rowsPerBlockCount.emplace_back(buffers[bucket].size());
                pagetoWriteto++;
                buffers[bucket].clear();
            }
        }
        buffers[k - 1].clear();
    }
    for (int i = 0; i < k - 1; i++)
    {
        if (buffers[i].size())
        {
            bufferManager.writePage(parsedQuery.joinFirstRelationName, pagetoWriteto, buffers[i], buffers[i].size(), 6, 0, "", buffers[i]);
            pageMapper[i].push_back(pagetoWriteto);
            table1->rowsPerBlockCount.emplace_back(buffers[i].size());
            pagetoWriteto++;
            buffers[i].clear();
        }
    }

    k = 10;
    Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    cursor = table2->getCursor();
    int hashElem2 = table2->getColumnIndex(parsedQuery.joinSecondColumnName);
    int pagetoWriteto1 = table2->blockCount;
    unordered_map<int, vector<int>> pageMapper1;
    for (int i = 0; i < table2->blockCount; i++)
    {
        cursor.nextPage(i);
        buffers[k - 1] = cursor.getNextBlock();
        for (int j = 0; j < buffers[k - 1].size(); j++)
        {
            int bucket = simpleHash(buffers[k - 1][j][hashElem2], k - 1);
            buffers[bucket].push_back(buffers[k - 1][j]);
            if (buffers[bucket].size() == table2->maxRowsPerBlock)
            {
                bufferManager.writePage(parsedQuery.joinSecondRelationName, pagetoWriteto1, buffers[bucket], buffers[bucket].size(), 6, 0, "", buffers[bucket]);
                table2->rowsPerBlockCount.emplace_back(buffers[bucket].size());
                pageMapper1[bucket].push_back(pagetoWriteto1);
                pagetoWriteto1++;
                buffers[bucket].clear();
            }
        }
        buffers[k - 1].clear();
    }
    for (int i = 0; i < k - 1; i++)
    {
        if (buffers[i].size())
        {
            bufferManager.writePage(parsedQuery.joinSecondRelationName, pagetoWriteto1, buffers[i], buffers[i].size(), 6, 0, "", buffers[i]);
            table2->rowsPerBlockCount.emplace_back(buffers[i].size());
            pageMapper1[i].push_back(pagetoWriteto1);
            pagetoWriteto1++;
            buffers[i].clear();
        }
    }
    vector<string> newColumns = table1->columns;
    newColumns.insert(newColumns.end(), table2->columns.begin(), table2->columns.end());
    Table *resultantTable = new Table(parsedQuery.joinResultRelationName, newColumns);

    Cursor table1cursor = table1->getCursor();
    int countrows = 0;
    Cursor table2cursor = table2->getCursor();
    for (const auto &[key, vec] : pageMapper)
    {
        if (pageMapper1.find(key) != pageMapper1.end())
        {
            for (const int &val1 : vec)
            {
                for (const int &val2 : pageMapper1[key])
                {

                    table1cursor.nextPage(val1);
                    buffers[0] = table1cursor.getNextBlock();

                    table2cursor.nextPage(val2);
                    buffers[1] = table2cursor.getNextBlock();

                    // this loops through each row in the blocks
                    for (int i = 0; i < buffers[0].size(); i++)
                    {
                        for (int j = 0; j < buffers[1].size(); j++)
                        {
                            if (buffers[0][i][hashElem1] == buffers[1][j][hashElem2])
                            {
                                vector<int> row1 = buffers[0][i];
                                row1.insert(row1.end(), buffers[1][j].begin(), buffers[1][j].end());
                                resultantTable->writeRow(row1);
                                countrows++;
                            }
                        }
                    }
                    buffers[0].clear();
                    buffers[1].clear();
                }
            }
        }
    }

    if (countrows == 0)
    {
        string sourceFileName = "../data/temp/" + parsedQuery.joinResultRelationName + ".csv";
        bufferManager.deleteFile(sourceFileName);
        return;
    }

    resultantTable->blockify();
    tableCatalogue.insertTable(resultantTable);

    for (int i = table1->blockCount; i < pagetoWriteto; i++)
    {
        bufferManager.deleteFile(parsedQuery.joinFirstRelationName, i);
    }

    for (int i = table2->blockCount; i < pagetoWriteto1; i++)
    {
        bufferManager.deleteFile(parsedQuery.joinSecondRelationName, i);
    }

    parsedQuery.sortRelationName=resultantTable->tableName;
    parsedQuery.sortColumns.clear();
    parsedQuery.sortColumns.push_back(parsedQuery.joinFirstColumnName);
    parsedQuery.sortingStrategies.clear();
    parsedQuery.sortingStrategies.push_back(ASC);

    
    executeSORT(); 
    bufferManager.clearCSV(resultantTable->tableName);
    Cursor csvWriter=resultantTable->getCursor();
    for(int i=0;i<resultantTable->blockCount;i++)
    {
        buffers[0].clear();
        csvWriter.nextPage(i);
        buffers[0]=csvWriter.getNextBlock();
        for(int row=0;row<buffers[0].size();row++)
        {
            resultantTable->writeRow(buffers[0][row]);
        }
    }

    return;
}