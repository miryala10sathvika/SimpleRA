#include "global.h"

/**
 * @brief
 * SYNTAX: DELETE FROM relation_name WHERE column_name bin_op int_literal
 */

bool syntacticParseDELETE()
{
    logger.log("syntacticParseDELETE");

    if (tokenizedQuery.size() != 7 || tokenizedQuery[0] != "DELETE" || tokenizedQuery[1] != "FROM" || tokenizedQuery[3] != "WHERE")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = DELETE;
    parsedQuery.deleteRelationName = tokenizedQuery[2];

    parsedQuery.deleteColumnName = tokenizedQuery[4];
    string binaryOperator = tokenizedQuery[5];
    string secondArgument = tokenizedQuery[6];

    if (binaryOperator == "<")
        parsedQuery.deleteBinaryOperator = LESS_THAN;
    else if (binaryOperator == "<=")
        parsedQuery.deleteBinaryOperator = LEQ;
    else if (binaryOperator == ">")
        parsedQuery.deleteBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=")
        parsedQuery.deleteBinaryOperator = GEQ;
    else if (binaryOperator == "==")
        parsedQuery.deleteBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.deleteBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    regex numeric("[-]?[0-9]+");
    if (regex_match(secondArgument, numeric))
    {
        parsedQuery.deleteIntLiteral = stoi(secondArgument);
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}
void printDeletionMap(const std::unordered_map<int, std::set<int>> &DeletionMap)
{
    // Iterate through each key-value pair in the unordered_map
    for (const auto &pair : DeletionMap)
    {
        std::cout << "Key: " << pair.first << " -> Values: ";

        // Iterate through the set associated with the current key
        for (const auto &value : pair.second)
        {
            std::cout << value << " ";
        }

        std::cout << std::endl; // Move to the next line after printing the set
    }
}
bool semanticParseDELETE()
{
    logger.log("semanticParseDELETE");
    if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.deleteColumnName, parsedQuery.deleteRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void delete1(int rowVal, vector<vector<int>> Block, Cursor L2_Cursor, int flag, Table *SourceTable)
{
    vector<vector<int>> L2Block;

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

    // looping through the L1 block rows
    for (int i = start; i < end; i++)
    {
       
        // check this condition - for not equal to
        if (Block[i][0] == parsedQuery.deleteIntLiteral && flag == 5)
        {
         
            continue;
        }
        for (int j = 1; j < Block[i].size(); j++)
        {
            // gets the L2 block index for Block[i]
            int rowDetails = Block[i][j];

            if (rowDetails == -1)
            {
                break;
            }

            L2_Cursor.nextPage(rowDetails);
            L2Block = L2_Cursor.getNextBlock();

            // getting the l2 block from the l1 index
            for (int k = 0; k < L2Block.size(); k++)
            {
                int blockIndex = L2Block[k][0];
                int currIndex = k;
                // finding all the rows in the l2 block with the same block index
                while (currIndex < L2Block.size() && blockIndex == L2Block[currIndex][0])
                {
                    // if()
                    SourceTable->DeletionMap[blockIndex].insert(L2Block[currIndex][1]);
                    SourceTable->deleteCount++;
                    currIndex++;
                }
                k = currIndex-1;
            }
            L2Block.clear();
        }
    }
  //  cout << "Table Name: " << SourceTable->tableName << endl;
   // printDeletionMap(SourceTable->DeletionMap);
}
void printvec(vector<uint> t)
{
    cout << "PRINTING ROWS PER BLOCK" << endl;
    for (auto elem : t)
        cout << elem << " ";
    cout << endl;
}

void deleteRowsFromTable_Threshold(Table *table)
{
    // cout << "IN THRESHOLD BEFORE DELETION" << endl;
    // printvec(table->rowsPerBlockCount);
    Cursor c = table->getCursor();
    vector<vector<int>> Buffer;
    vector<vector<int>> temp;
    Buffer.clear();
    int rowCount = 0;
    int pagetoWriteTo = table->blockCount;
    for (int i = 0; i < table->blockCount; i++)
    {
        if (table->rowsPerBlockCount[i] == 0)
        {
            continue;
        }
        c.nextPage(i);
        Buffer.clear();
        Buffer = c.getNextBlock();
        for (int j = 0; j < Buffer.size(); j++)
        {
            temp.push_back(Buffer[j]);
            rowCount++;
            if (temp.size() == table->maxRowsPerBlock)
            {
                bufferManager.writePage(table->tableName, pagetoWriteTo, temp, temp.size(), 7, 0, "", Buffer);
                table->rowsPerBlockCount.emplace_back(temp.size());
                pagetoWriteTo++;
                temp.clear();
            }
        }
    }
    if (temp.size() > 0)
    {
        bufferManager.writePage(table->tableName, pagetoWriteTo, temp, temp.size(), 7, 0, "", Buffer);
        table->rowsPerBlockCount.emplace_back(temp.size());
        pagetoWriteTo++;
        temp.clear();
    }
    temp.clear();
    // cout << "Overwriting"<< endl;
    for (int i = table->blockCount; i < pagetoWriteTo; i++)
    {
        c.nextPage(i);
        temp.clear();
        temp = c.getNextBlock();
        bufferManager.writePage(table->tableName, i - table->blockCount, temp, temp.size(), 7, 0, "", temp);
        table->rowsPerBlockCount[i - table->blockCount] = temp.size();
    }
    // deleting the extra files
    if (pagetoWriteTo != 0)
    {
        for (int i = pagetoWriteTo - table->blockCount; i < pagetoWriteTo; i++)
        {

            bufferManager.deleteFile(table->tableName, i);
            table->rowsPerBlockCount[i] = -1;
        }
        table->rowsPerBlockCount.erase(remove(table->rowsPerBlockCount.begin(), table->rowsPerBlockCount.end(), -1), table->rowsPerBlockCount.end());
        // printvec(table->rowsPerBlockCount);
    }
    if (rowCount == 0)
    {
        for(int p0=0;p0<table->columnCount;p0++)
        {
            string L1Name=table->tableName+table->columns[p0]+"L1";
            string L2Name=table->tableName+table->columns[p0]+"L2";
            if(tableCatalogue.isTable(L1Name))
            tableCatalogue.deleteTable(L1Name);
            if(tableCatalogue.isTable(L2Name))
            tableCatalogue.deleteTable(L2Name);
        }
        table->blockCount = 0;
        tableCatalogue.deleteTable(table->tableName);

        if (!tableCatalogue.isTable(table->tableName))
            return;
    }
    table->rowCount = rowCount;
    table->blockCount = pagetoWriteTo - table->blockCount;
    table->deleteCount=0;
    // cout << "IN THRESHOLD AFTER DELETION" << endl;
    // printvec(table->rowsPerBlockCount);
}

void deleteRowsFromTable_Blocks(Table *table)
{
   // cout << "IN DELETEROWSFROMBLOCKS BEFORE DELETION" << endl;

    // printvec(table->rowsPerBlockCount);
    Cursor c = table->getCursor();
    vector<vector<int>> temp;
    vector<vector<int>> towriteback;
    towriteback.clear();
    temp.clear();
    for (int i = 0; i < table->blockCount; i++)
    {
        c.nextPage(i);
        temp.clear();
        temp = c.getNextBlock();
        for (int j = 0; j < temp.size(); j++)
        {
            if (table->DeletionMap[i].find(j) == table->DeletionMap[i].end())
            {
                // cout << "Hello" << endl;

                towriteback.push_back(temp[j]);
                // printvec(temp[j]);
            }
        }
        // cout << "FOR BLOCK " << i << endl;
        // for(auto row:towriteback)
        // printvec(row);
        if (towriteback.size() == 0)
        {
           
            table->rowsPerBlockCount[i] = 0;
            towriteback.clear();
        }
        else if (towriteback.size() > 0)
        {
            // cout << "For block:  " << i << endl;
            // cout << "Before Writing to Page  " << " The size to be written is  " << towriteback.size() << endl;
            bufferManager.writePage(table->tableName, i, towriteback, towriteback.size(), 7, 0, "", towriteback);
            table->rowsPerBlockCount[i] = towriteback.size();
            towriteback.clear();
        }
    }
  //  printDeletionMap(table->DeletionMap);
    table->DeletionMap.clear();
  //  cout << "IN DELETEROWSFROMBLOCKS AFTER DELETION" << endl;

    // printvec(table->rowsPerBlockCount);
}

void buildIndex(Table *table, string indexName)
{
    string L1IndexName = table->tableName + indexName + "L1";
    string L2IndexName = table->tableName + indexName + "L2";

    Table *l1table = tableCatalogue.getTable(L1IndexName);
    if (l1table != NULL)
    {
        for (int i = 0; i < l1table->blockCount; i++)
        {
            bufferManager.deleteFile(l1table->tableName, i);
        }
        tableCatalogue.deleteTable(L1IndexName);
    }

    Table *l2table = tableCatalogue.getTable(L2IndexName);
    if (l2table != NULL)
    {
        for (int i = 0; i < l2table->blockCount; i++)
        {
            bufferManager.deleteFile(l2table->tableName, i);
        }
        tableCatalogue.deleteTable(L2IndexName);
    }

    parsedQuery.indexColumnName = indexName;
    parsedQuery.indexRelationName = table->tableName;
    executeINDEX();
}

void executeDELETE()
{
    
    logger.log("executeDELETE");
    parsedQuery.indexRelationName = parsedQuery.deleteRelationName;

    parsedQuery.indexColumnName = parsedQuery.deleteColumnName;
    Table *sourceTable = tableCatalogue.getTable(parsedQuery.deleteRelationName);

    string L1tableName = parsedQuery.deleteRelationName + parsedQuery.deleteColumnName + "L1";
    Table *indexTable = tableCatalogue.getTable(L1tableName);

    if (indexTable == NULL)
    {
        executeINDEX();
    }
  
    Table *indexTableL1 = tableCatalogue.getTable(L1tableName);
    parsedQuery.searchBinaryOperator = parsedQuery.deleteBinaryOperator;
     int BlockIndexL1;
    if (parsedQuery.deleteBinaryOperator != 5)
        BlockIndexL1 = binarySearcher(indexTableL1, parsedQuery.deleteIntLiteral);
    if (BlockIndexL1 == -1)
        return;

    vector<vector<int>> Block;
    int rowVal = -1;
    

    if (parsedQuery.deleteBinaryOperator == GREATER_THAN || parsedQuery.deleteBinaryOperator == GEQ)
    {
        Cursor c = indexTableL1->getCursor();
        c.nextPage(BlockIndexL1);
        Block.clear();
        Block = c.getNextBlock();
        for (int i = 0; i < Block.size(); i++)
        {

            if (Block[i][0] > parsedQuery.deleteIntLiteral && parsedQuery.deleteBinaryOperator == GREATER_THAN)
            {
                rowVal = i;
                break;
            }
            else if (Block[i][0] >= parsedQuery.deleteIntLiteral && parsedQuery.deleteBinaryOperator == GEQ)
            {
                rowVal = i;
                break;
            }
        }
        if (rowVal == -1)
        {
            rowVal = 0;
        }
        string L2tableName = parsedQuery.deleteRelationName + parsedQuery.deleteColumnName + "L2";
        Table *indexTableL2 = tableCatalogue.getTable(L2tableName);
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();

        int respagecount = 0;

        delete1(rowVal, Block, L2_Cursor, 1, sourceTable);
        // deletes all the rows from the L1 blocks corresponding to the condition
        for (int r = BlockIndexL1 + 1; r < indexTableL1->blockCount; r++)
        {
            c.nextPage(r);
            Block.clear();
            Block = c.getNextBlock();
            delete1(0, Block, L2_Cursor, 1, sourceTable);
        }
        c.nextPage(BlockIndexL1);
        Block.clear();
        Block = c.getNextBlock();
    }

    if (parsedQuery.deleteBinaryOperator == LESS_THAN || parsedQuery.deleteBinaryOperator == LEQ)
    {
        rowVal = -1;
        Cursor c = indexTableL1->getCursor();
        c.nextPage(BlockIndexL1);
        Block.clear();
        Block = c.getNextBlock();

        for (int i = 0; i < Block.size(); i++)
        {
            if (Block[i][0] >= parsedQuery.deleteIntLiteral && parsedQuery.deleteBinaryOperator == LESS_THAN)
            {
                if (Block[i][0] == parsedQuery.deleteIntLiteral)
                {
                    rowVal = i;
                    break;
                }
                else if (Block[i][0] > parsedQuery.deleteIntLiteral)
                {
                    rowVal = i;
                    break;
                }
            }
            else if (Block[i][0] >= parsedQuery.deleteIntLiteral && parsedQuery.deleteBinaryOperator == LEQ)
            {
                if (Block[i][0] == parsedQuery.deleteIntLiteral)
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
        string L2tableName = parsedQuery.deleteRelationName + parsedQuery.deleteColumnName + "L2";
        Table *indexTableL2 = tableCatalogue.getTable(L2tableName);
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();

        int respagecount = 0;
        if (BlockIndexL1 == 1)
        {
            c.nextPage(0);
            Block.clear();
            Block = c.getNextBlock();
            delete1(0, Block, L2_Cursor, 1, sourceTable);

            c.nextPage(1);
            Block.clear();
            Block = c.getNextBlock();
            delete1(rowVal, Block, L2_Cursor, 0, sourceTable);
        }
        else
        {

            // deletes from the table
            for (int r = 0; r < BlockIndexL1 - 1; r++)
            {
                c.nextPage(r);
                Block.clear();
                Block = c.getNextBlock();
                delete1(0, Block, L2_Cursor, 1, sourceTable);
            }
            delete1(rowVal, Block, L2_Cursor, 0, sourceTable);
        }
    }
    else if (parsedQuery.deleteBinaryOperator == EQUAL)
    {
        rowVal = -1;
        Cursor c = indexTableL1->getCursor();
        c.nextPage(BlockIndexL1);
        Block.clear();
        Block = c.getNextBlock();
        string L2tableName = parsedQuery.deleteRelationName + parsedQuery.deleteColumnName + "L2";
        Table *indexTableL2 = tableCatalogue.getTable(L2tableName);
        for (int i = 0; i < Block.size(); i++)
        {
            if (Block[i][0] == parsedQuery.deleteIntLiteral)
            {
                rowVal = i;
                break;
            }
        }
        if (rowVal == -1)
        {
            string sourceFileName = "../data/temp/" + sourceTable->tableName + ".csv";
            bufferManager.deleteFile(sourceFileName);
            return;
        }
       
        Cursor Source_Cursor = sourceTable->getCursor();
        Cursor L2_Cursor = indexTableL2->getCursor();
        delete1(rowVal, Block, L2_Cursor, 3, sourceTable);
        // search(rowVal, Block, Source_Cursor, L2_Cursor, resultantTable, &respagecount, sourceTable, 3);
    }


    else if (parsedQuery.deleteBinaryOperator == NOT_EQUAL)
    {

     
        Cursor Source_Cursor = sourceTable->getCursor();
        string L2tableName = parsedQuery.deleteRelationName + parsedQuery.deleteColumnName + "L2";
        Table *indexTableL2 = tableCatalogue.getTable(L2tableName);
    
        Cursor L2_Cursor = indexTableL2->getCursor();
        Cursor L1_Cursor = indexTableL1->getCursor();
        for (int i = 0; i < indexTableL1->blockCount; i++)
        {
            L1_Cursor.nextPage(i);
            Block.clear();
            Block = L1_Cursor.getNextBlock();
            delete1(0, Block, L2_Cursor, 5, sourceTable);
            // search(0, Block, Source_Cursor, L2_Cursor, sourceTable, &respagecount, sourceTable, 5);
        }
    }

    if(sourceTable->deleteCount==sourceTable->rowCount)
    {
        for(int p0=0;p0<sourceTable->columnCount;p0++)
        {
            string L1Name=sourceTable->tableName+sourceTable->columns[p0]+"L1";
            string L2Name=sourceTable->tableName+sourceTable->columns[p0]+"L2";
            if(tableCatalogue.isTable(L1Name))
            tableCatalogue.deleteTable(L1Name);
            if(tableCatalogue.isTable(L2Name))
            tableCatalogue.deleteTable(L2Name);
        }
        tableCatalogue.deleteTable(sourceTable->tableName);
    }
    return;
}