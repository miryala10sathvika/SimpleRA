#include "global.h"
/**
 * @brief
 * SYNTAX: R <- SEARCH FROM relation_name WHERE column_name bin_op int_literal
 */
bool syntacticParseSEARCH()
{
    logger.log("syntacticParseSEARCH");

    // Expected format: res_table <- SEARCH FROM table_name WHERE column operator value
    if (tokenizedQuery.size() != 9 || tokenizedQuery[1] != "<-" || tokenizedQuery[2] != "SEARCH" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "WHERE")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SEARCH;
    parsedQuery.searchResultRelationName = tokenizedQuery[0];
    parsedQuery.searchRelationName = tokenizedQuery[4];

    parsedQuery.searchFirstColumnName = tokenizedQuery[6];
    string binaryOperator = tokenizedQuery[7];
    string secondArgument = tokenizedQuery[8];

    if (binaryOperator == "<")
        parsedQuery.searchBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.searchBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.searchBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.searchBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.searchBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.searchBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    regex numeric("[-]?[0-9]+");
    if (regex_match(secondArgument, numeric))
    {

        parsedQuery.searchIntLiteral = stoi(secondArgument);
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseSEARCH()
{
    logger.log("semanticParseSELECTION");

    if (tableCatalogue.isTable(parsedQuery.searchResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.searchRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.searchFirstColumnName, parsedQuery.searchRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

int binarySearcher(Table *table1, int searchVal)
{
    Cursor cursor = table1->getCursor();
    int start = 0;
    int end = table1->blockCount - 1;
    vector<vector<int>> block1;
    int mid = -1;
    int flag = 0;
    while (start <= end)
    {
        mid = (start + end) / 2;
        cursor.nextPage(mid);
        block1 = cursor.getNextBlock();

        int first = block1[0][0];
        int last = block1[block1.size() - 1][0];

        switch (parsedQuery.searchBinaryOperator)
        {
        case EQUAL:
            if (searchVal >= first && searchVal <= last)
                return mid;
            if (searchVal < first)
                end = mid - 1;
            else
                start = mid + 1;
            break;

        case LESS_THAN:
            if (searchVal <= first)
                end = mid - 1;
            else
                start = mid + 1;
            break;

        case GREATER_THAN:
            if (searchVal >= last)
                start = mid + 1;
            else
                end = mid - 1;
            break;

        case LEQ:
            if (searchVal < first)
                end = mid - 1;
            else if (searchVal <= last)
                return mid;
            else
                start = mid + 1;
            break;

        case GEQ:
            if (searchVal > last)
                start = mid + 1;
            else if (searchVal >= first)
                return mid;
            else
                end = mid - 1;
            break;
        default:
            break;
        }
    }

    if (parsedQuery.searchBinaryOperator == GEQ || parsedQuery.searchBinaryOperator == GREATER_THAN)
    {
        if (start >= table1->blockCount)
            return -1;

        cursor.nextPage(start);
        block1 = cursor.getNextBlock();
        if (parsedQuery.searchBinaryOperator == GEQ && block1[block1.size() - 1][0] >= searchVal)
            return start;
        if (parsedQuery.searchBinaryOperator == GREATER_THAN && block1[block1.size() - 1][0] > searchVal)
            return start;
        return -1;
    }
    else if (parsedQuery.searchBinaryOperator == LEQ || parsedQuery.searchBinaryOperator == LESS_THAN)
    {
        if (end < 0)
            return -1;

        cursor.nextPage(end);
        block1 = cursor.getNextBlock();
        if (parsedQuery.searchBinaryOperator == LEQ && block1[0][0] <= searchVal)
            return end;
        if (parsedQuery.searchBinaryOperator == LESS_THAN && block1[0][0] < searchVal)
            return end;
        return -1;
    }
    return -1;
}

vector<vector<int>> Buffer;

void search(int rowVal, vector<vector<int>> Block, Cursor Source_Cursor, Cursor L2_Cursor, Table *resultantTable, int *resultPageCount, Table *sourceTable, int flag)
{
    vector<vector<int>> L2Block;
    vector<vector<int>> TableBlock;

    int start, end;
    if (flag == 1 || flag == 5)
    {
        start = rowVal;
        end = Block.size();
    }
    else if (flag == 0)
    {
        start = 0;
        end = rowVal;
    }
    else
    {
        start = rowVal;
        end = rowVal + 1;
    }

    for (int i = start; i < end; i++)
    {
        if (Block[i][0] == parsedQuery.searchIntLiteral && flag == 5)
        {
            continue;
        }
        for (int j = 1; j < Block[i].size(); j++)
        {
            int rowDetails = Block[i][j];

            if (rowDetails == -1)
            {
                //  cout<<"it was -1"<<endl;
                break;
            }
            L2_Cursor.nextPage(rowDetails);
            L2Block = L2_Cursor.getNextBlock();

            // getting the l2 block from the l1 index
            for (int k = 0; k < L2Block.size(); k++)
            {
                int blockIndex = L2Block[k][0];
                int currIndex = k;
                vector<int> rowIndices;
                // finding all the rows in the l2 block with the same block index
                while (currIndex < L2Block.size() && blockIndex == L2Block[currIndex][0])
                {
                    rowIndices.push_back(L2Block[currIndex][1]);
                    currIndex++;
                }
                // cout << "Row indices" << endl;
                // for (int ll = 0; ll < rowIndices.size(); ll++)
                // {
                //     cout << rowIndices[ll] << " ";
                // }
                // cout << endl;

                k = currIndex - 1;
                Source_Cursor.nextPage(blockIndex);
                TableBlock = Source_Cursor.getNextBlock();

                // writing to the table
                for (int h = 0; h < rowIndices.size(); h++)
                {
                    // cout << "Getting the values" <<endl;
                    // for(int l=0;l<TableBlock[rowIndices[h]].size();l++)
                    // cout << TableBlock[rowIndices[h]][l]<<"  ";
                    // cout << endl;
                    Buffer.push_back(TableBlock[rowIndices[h]]);
                    resultantTable->writeRow(TableBlock[rowIndices[h]]);
                    if (Buffer.size() == sourceTable->maxRowsPerBlock)
                    {
                        bufferManager.writePage(parsedQuery.searchResultRelationName, *resultPageCount, Buffer, Buffer.size(), 7, 0, parsedQuery.searchResultRelationName, Buffer);
                        resultantTable->rowsPerBlockCount.emplace_back(Buffer.size());
                        Buffer.clear();
                        (*resultPageCount)++;
                    }
                }
                TableBlock.clear();
            }
            L2Block.clear();
        }
    }
}

void executeSEARCH()
{
    Buffer.clear();
    logger.log("executeSEARCH");
    // cout << parsedQuery.searchBinaryOperator << " **" << endl;
    parsedQuery.indexRelationName = parsedQuery.searchRelationName;
    parsedQuery.indexColumnName = parsedQuery.searchFirstColumnName;
    Table *sourceTable = tableCatalogue.getTable(parsedQuery.searchRelationName);

    string L1tableName = parsedQuery.searchRelationName + parsedQuery.searchFirstColumnName + "L1";
    Table *indexTable = tableCatalogue.getTable(L1tableName);
    if (indexTable == NULL)
    {
        executeINDEX();
    }
    string L2tableName = parsedQuery.searchRelationName + parsedQuery.searchFirstColumnName + "L2";

    vector<vector<int>> block1;

    Table *indexTableL1 = tableCatalogue.getTable(L1tableName);
    Table *indexTableL2 = tableCatalogue.getTable(L2tableName);
    int blockIndexL1;
    if (parsedQuery.searchBinaryOperator != 5)
        blockIndexL1 = binarySearcher(indexTableL1, parsedQuery.searchIntLiteral);
    if (blockIndexL1 == -1)
        return;

    Table *resultantTable = new Table(parsedQuery.searchResultRelationName, sourceTable->columns);
    vector<vector<int>> Block;
    int rowVal = -1;

    if (parsedQuery.searchBinaryOperator == GREATER_THAN || parsedQuery.searchBinaryOperator == GEQ)
    {
        Cursor c = indexTableL1->getCursor();
        c.nextPage(blockIndexL1);
        Block.clear();
        Block = c.getNextBlock();
        for (int i = 0; i < Block.size(); i++)
        {
            if (Block[i][0] > parsedQuery.searchIntLiteral && parsedQuery.searchBinaryOperator == GREATER_THAN)
            {
                rowVal = i;
                break;
            }
            else if (Block[i][0] >= parsedQuery.searchIntLiteral && parsedQuery.searchBinaryOperator == GEQ)
            {
                rowVal = i;
                break;
            }
        }
        if (rowVal == -1)
        {
            rowVal = 0;
        }
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();

        int respagecount = 0;

        // this is for the starting block
        search(rowVal, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 1);

        // this is for the rest of the blocks
        for (int r = blockIndexL1 + 1; r < indexTableL1->blockCount; r++)
        {
            c.nextPage(r);
            Block.clear();
            Block = c.getNextBlock();
            search(0, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 1);
        }
        if (Buffer.size() > 0)
        {
            bufferManager.writePage(parsedQuery.searchResultRelationName, respagecount, Buffer, Buffer.size(), 7, 0, parsedQuery.searchResultRelationName, Buffer);
            resultantTable->rowsPerBlockCount.emplace_back(Buffer.size());
            Buffer.clear();
            respagecount++;
        }
        // change made
        if (respagecount > 0)
        {
            resultantTable->blockify();
            tableCatalogue.insertTable(resultantTable);
            resultantTable->blockCount = respagecount;
            respagecount = 0;
        }
    }

    else if (parsedQuery.searchBinaryOperator == LESS_THAN || parsedQuery.searchBinaryOperator == LEQ)
    {
        rowVal = -1;
        Cursor c = indexTableL1->getCursor();
        c.nextPage(blockIndexL1);
        Block.clear();
        Block = c.getNextBlock();
        for (int i = 0; i < Block.size(); i++)
        {
            if (Block[i][0] >= parsedQuery.searchIntLiteral && parsedQuery.searchBinaryOperator == LESS_THAN)
            {
                if (Block[i][0] == parsedQuery.searchIntLiteral)
                {
                    rowVal = i;
                    break;
                }
                else if (Block[i][0] > parsedQuery.searchIntLiteral)
                {
                    rowVal = i;
                    break;
                }
            }
            else if (Block[i][0] >= parsedQuery.searchIntLiteral && parsedQuery.searchBinaryOperator == LEQ)
            {
                if (Block[i][0] == parsedQuery.searchIntLiteral)
                    rowVal = i + 1;
                else
                    rowVal = i;
                break;
            }
        }
        if (rowVal == -1)
        {
            rowVal = Block.size();
        }
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();

        int respagecount = 0;
        // cout << "Row val: " << rowVal << endl;
        // cout << "BlockIndexL1: " << blockIndexL1 << endl;
        if (blockIndexL1 == 1)
        {

            c.nextPage(0);
            Block.clear();
            Block = c.getNextBlock();
            search(0, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 1);

            c.nextPage(1);
            Block.clear();
            Block = c.getNextBlock();

            search(rowVal, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 0);
        }
        else
        {
            for (int r = 0; r < blockIndexL1 - 1; r++)
            {
                c.nextPage(r);
                Block.clear();
                Block = c.getNextBlock();
                search(0, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 1);
            }

            search(rowVal, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 0);
        }

        if (Buffer.size() > 0)
        {
            bufferManager.writePage(parsedQuery.searchResultRelationName, respagecount, Buffer, Buffer.size(), 7, 0, parsedQuery.searchResultRelationName, Buffer);
            resultantTable->rowsPerBlockCount.emplace_back(Buffer.size());
            Buffer.clear();
            respagecount++;
        }
        // change made
        if (respagecount > 0)
        {
            resultantTable->blockify();
            tableCatalogue.insertTable(resultantTable);
            resultantTable->blockCount = respagecount;
            respagecount = 0;
        }
    }
    else if (parsedQuery.searchBinaryOperator == EQUAL)
    {
        rowVal = -1;
        int respagecount = 0;
        Cursor c = indexTableL1->getCursor();
        c.nextPage(blockIndexL1);
        Block.clear();
        Block = c.getNextBlock();

        for (int i = 0; i < Block.size(); i++)
        {
            if (Block[i][0] == parsedQuery.searchIntLiteral)
            {
                rowVal = i;
                break;
            }
        }

        if (rowVal == -1)
        {
            string sourceFileName = "../data/temp/" + resultantTable->tableName + ".csv";
            bufferManager.deleteFile(sourceFileName);
            return;
        }

        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();
        search(rowVal, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 3);
        if (Buffer.size() > 0)
        {
            bufferManager.writePage(parsedQuery.searchResultRelationName, respagecount, Buffer, Buffer.size(), 7, 0, parsedQuery.searchResultRelationName, Buffer);
            resultantTable->rowsPerBlockCount.emplace_back(Buffer.size());
            Buffer.clear();
            respagecount++;
        }
        // change made
        if (respagecount > 0)
        {
            resultantTable->blockify();
            tableCatalogue.insertTable(resultantTable);
            resultantTable->blockCount = respagecount;
            respagecount = 0;
        }
    }

    else if (parsedQuery.searchBinaryOperator == NOT_EQUAL)
    {
        int respagecount = 0;
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();
        Cursor L1_Cursor = indexTableL1->getCursor();
        for (int i = 0; i < indexTableL1->blockCount; i++)
        {
            L1_Cursor.nextPage(i);
            Block.clear();
            Block = L1_Cursor.getNextBlock();
            search(0, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 5);
        }
        if (Buffer.size() > 0)
        {
            bufferManager.writePage(parsedQuery.searchResultRelationName, respagecount, Buffer, Buffer.size(), 7, 0, parsedQuery.searchResultRelationName, Buffer);
            resultantTable->rowsPerBlockCount.emplace_back(Buffer.size());
            Buffer.clear();
            respagecount++;
        }
        // change made
        if (respagecount > 0)
        {
            resultantTable->blockify();
            tableCatalogue.insertTable(resultantTable);
            resultantTable->blockCount = respagecount;
            respagecount = 0;
        }
    }
    return;
}