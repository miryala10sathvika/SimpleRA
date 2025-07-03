#include "global.h"

/**
 * @brief
 * SYNTAX: INSERT INTO table_name ( col1 = val1, col2 = val2, col3 = val3 … )
 */

bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    parsedQuery.insertColumns.clear();
    parsedQuery.insertValues.clear();
    if (tokenizedQuery[0] != "INSERT" || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "(")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = INSERT;
    parsedQuery.insertrelationName = tokenizedQuery[2];

    int i = 4;
    while (i + 2 < tokenizedQuery.size() && tokenizedQuery[i] != ")")
    {
        string column = tokenizedQuery[i];
        string equalsSign = tokenizedQuery[i + 1];
        string valueStr = tokenizedQuery[i + 2];

        if (equalsSign != "=")
        {
            cout << "SYNTAX ERROR: Expected '=' after column name" << endl;
            return false;
        }

        int value;
        try
        {
            value = stoi(valueStr);
        }
        catch (const std::exception &e)
        {
            cout << "SYNTAX ERROR: Invalid value for column '" << column << "'" << endl;
            return false;
        }

        parsedQuery.insertColumns.push_back(column);
        parsedQuery.insertValues.push_back(value);

        i += 3;
        if (i < tokenizedQuery.size() && tokenizedQuery[i] == ",")
        {
            i++;
        }
    }

    if (i >= tokenizedQuery.size() || tokenizedQuery[i] != ")")
    {
        cout << "SYNTAX ERROR: Missing closing parenthesis" << endl;
        return false;
    }

    return true;
}

bool semanticParseINSERT()
{
    logger.log("semanticParseINSERT");
    if (!tableCatalogue.isTable(parsedQuery.insertrelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table* table = tableCatalogue.getTable(parsedQuery.insertrelationName);
    for(int i=0;i<parsedQuery.insertColumns.size();i++)
    {
        if (!tableCatalogue.isColumnFromTable(parsedQuery.insertColumns[i], parsedQuery.insertrelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }
    return true;
}





void executeINSERT()
{
    logger.log("executeINSERT");
    Table *table = tableCatalogue.getTable(parsedQuery.insertrelationName);

    vector<int> row(table->columnCount, 0);
    for (int i = 0; i < parsedQuery.insertColumns.size(); i++) {
        int colIdx = table->getColumnIndex(parsedQuery.insertColumns[i]);
        row[colIdx] = parsedQuery.insertValues[i];
    }
    int newPage, newSlot;

    if (table->blockCount == 0) {
        vector<vector<int>> block = { row };
        bufferManager.writePage(parsedQuery.insertrelationName, 0, block, 1, 7, 0,
                                parsedQuery.insertrelationName, block);
        table->rowsPerBlockCount.push_back(1);
        table->blockCount = 1;
        newPage = 0;
        newSlot = 0;
    } else {
        Cursor c = table->getCursor();
        int last = table->blockCount - 1;
        c.nextPage(last);
        auto block = c.getNextBlock();
        
        if ((int)block.size() == table->maxRowsPerBlock) {
            vector<vector<int>> newBlock = { row };
            newPage = table->blockCount;
            newSlot = 0;
            bufferManager.writePage(parsedQuery.insertrelationName, newPage,
                                    newBlock, 1, 7, 0,
                                    parsedQuery.insertrelationName, newBlock);
            table->rowsPerBlockCount.push_back(1);
            table->blockCount++;
        } else {
            block.push_back(row);
            newPage = last;
            newSlot = (int)block.size() - 1;
            bufferManager.writePage(parsedQuery.insertrelationName, last,
                                    block, block.size(), 7, 0,
                                    parsedQuery.insertrelationName, block);
            table->rowsPerBlockCount[last] = block.size();
        }
    }
    if (table->dirtyIndex == true){
        table->rowCount++;
        return ;
    }
    for (auto &colName : table->columns) {
        cout << "new column " << colName <<  endl;
        string L1name = table->tableName + colName + "L1";
        string L2name = table->tableName + colName + "L2";
        if (!tableCatalogue.isTable(L1name) || !tableCatalogue.isTable(L2name))
            continue;

        Table *L1 = tableCatalogue.getTable(L1name);
        Table *L2 = tableCatalogue.getTable(L2name);
        int key = row[table->getColumnIndex(colName)];

        // parsedQuery.searchBinaryOperator = EQUAL;
        // int l1Idx = binarySearcher(L1, key);
        // int pageToLoad = (l1Idx >= 0 ? l1Idx : L1->blockCount - 1);
        int pageToLoad = -1;
        bool check = false;
        for (int i = 0; i < L1->blockCount; i++) {
            Cursor tempCursor = L1->getCursor();
            tempCursor.nextPage(i);
            auto block = tempCursor.getNextBlock();
            if (!block.empty() && key <= block.back()[0]) {
                pageToLoad = i;
                Cursor ccheck = L1->getCursor();
                ccheck.nextPage(pageToLoad);
                auto l1Block = ccheck.getNextBlock();
                if ((int)l1Block.size() == (int)L1->maxRowsPerBlock){
                    tableCatalogue.deleteTable(L1name);
                    tableCatalogue.deleteTable(L2name);
                    logger.log("Index for column " + colName + " is full: dropped " + L1name + " & " + L2name);
                    check=true;
                }
                break;
            }
        }
        if(check){continue;}
        
        if (pageToLoad == -1) {
            pageToLoad = L1->blockCount - 1;
        }

        Cursor c1 = L1->getCursor();
        c1.nextPage(pageToLoad);
        auto l1Block = c1.getNextBlock();

        int off = -1;
        for (int r = 0; r < (int)l1Block.size(); r++) {
            if (l1Block[r][0] == key) { off = r; break; }
        }

        bool insertedToL2 = false;
        if (off >= 0) {
            auto &l1row = l1Block[off];
            int maxL2 = (int)L2->maxRowsPerBlock;
            for (int s = 1; s < (int)l1row.size(); s++) {
                int p = l1row[s];
                if (p < 0) continue;
                Cursor c2 = L2->getCursor();
                c2.nextPage(p);
                auto l2b = c2.getNextBlock();
                for (const auto &row : l2b) {
                        for (int val : row) {
                            cout << val << " ";
                        }
                        cout << endl;
                }
                if ((int)l2b.size() < maxL2) {
                    cout << "L2 Block Index: " << p << endl;
                    l2b.push_back({ newPage, newSlot });
                    for (const auto &row : l2b) {
                        for (int val : row) {
                            cout << val << " ";
                        }
                        cout << endl;
                    }
                    bufferManager.writePage(L2name, p, l2b, l2b.size(), 7, 0, L2name, l2b);
                    L2->rowsPerBlockCount[p] = l2b.size();
                    insertedToL2 = true;
                    break;
                }
            }
        }

        if (!insertedToL2) {
            if (off < 0) {
        vector<int> newL1(l1Block[0].size(), -1);
        newL1[0] = key;

        if ((int)l1Block.size() == (int)L1->maxRowsPerBlock) {
            std::sort(l1Block.begin(), l1Block.end(),
                    [](auto &a, auto &b){ return a[0] < b[0]; });
            bufferManager.writePage(
                L1name,
                pageToLoad,
                l1Block, l1Block.size(),
                7, 0,
                L1name,
                l1Block
            );

            int newL1Page = L1->blockCount;
            vector<vector<int>> freshL1 = { newL1 };
            bufferManager.writePage(
                L1name,
                newL1Page,
                freshL1, 1,
                7, 0,
                L1name,
                freshL1
            );
            L1->rowsPerBlockCount.push_back(1);
            L1->blockCount++;

            off = 0;
            l1Block = freshL1;
            pageToLoad = newL1Page;
        }
        else {
            l1Block.push_back(newL1);
            off = (int)l1Block.size() - 1;
        }
    }
            int newL2 = L2->blockCount;
            vector<vector<int>> fresh = {{ newPage, newSlot }};
            //print fresh
            cout << "fresh "  << endl;
            for (const auto &row : fresh) {
                for (int val : row) {
                    cout << val << " ";
                }
                cout << endl;
            }
            bufferManager.writePage(L2name, newL2, fresh, 1, 7, 0, L2name, fresh);
            L2->rowsPerBlockCount.push_back(1);
            L2->blockCount++;
            auto &r = l1Block[off];
            bool linked = false;
            for (int s = 1; s < (int)r.size(); s++) {
                if (r[s] < 0) {
                    r[s] = newL2;
                    linked = true;
                    break;
                }
            }
            if (!linked) {
                tableCatalogue.deleteTable(L1name);
                tableCatalogue.deleteTable(L2name);
                logger.log("Index for column " + colName + " is full: dropped " + L1name + " & " + L2name);
                continue;  
            }
        }
        std::sort(l1Block.begin(), l1Block.end(),
                  [](const vector<int>& a, const vector<int>& b) {
                      return a[0] < b[0];
                  });
        L1->rowsPerBlockCount[pageToLoad] = (int)l1Block.size();
        bufferManager.writePage(L1name, pageToLoad, l1Block, l1Block.size(),
                                7, 0, L1name, l1Block);
    }

    table->rowCount++;
}














    // // Update Indexes
    // for (auto it = table->indexcolumns.begin(); it != table->indexcolumns.end();)
    // {
    //     string colName = *it;
    //     string L1name = table->tableName + colName + "L1";
    //     string L2name = table->tableName + colName + "L2";

    //     if (!tableCatalogue.isTable(L1name) || !tableCatalogue.isTable(L2name)) {
    //         ++it;
    //         continue;
    //     }

    //     Table *L1 = tableCatalogue.getTable(L1name);
    //     Table *L2 = tableCatalogue.getTable(L2name);
    //     int key = row[table->getColumnIndex(colName)];

    //     // Step 1: Find or create L1 entry for the key
    //     parsedQuery.searchBinaryOperator = EQUAL;
    //     int l1BlockIdx = binarySearcher(L1, key);
    //     Cursor c1 = L1->getCursor();
    //     vector<vector<int>> l1Block;
    //     if (l1BlockIdx >= 0) {
    //         cout << "L1 Block Index: " << l1BlockIdx << endl;
    //         c1.nextPage(l1BlockIdx);
    //         l1Block = c1.getNextBlock();
    //         for (int i = 0; i < l1Block.size(); i++) {
    //             cout << "Row " << i << ": ";
    //             for (int j = 0; j < l1Block[i].size(); j++) {
    //                 cout << l1Block[i][j] << " ";
    //             }
    //             cout << endl;
    //         }
    //     } else {
    //         l1BlockIdx = L1->blockCount - 1;
    //         c1.nextPage(l1BlockIdx);
    //         l1Block = c1.getNextBlock();
    //     }

    //     int l1RowOff = -1;
    //     for (int r = 0; r < (int)l1Block.size(); r++) {
    //         if (l1Block[r][0] == key) {
    //             l1RowOff = r;
    //             break;
    //         }
    //     }

    //     if (l1RowOff < 0) {
    //         // Create new L1 row for the key
    //         vector<int> newL1row(l1Block[0].size(), -1);
    //         newL1row[0] = key;
    //         l1Block.push_back(newL1row);
    //         l1RowOff = l1Block.size() - 1;
            
    //         // Sort the L1 block to maintain order by key
    //         // This is important for binary search to work correctly
    //         sort(l1Block.begin(), l1Block.end(), 
    //             [](const vector<int>& a, const vector<int>& b) {
    //                 return a[0] < b[0];
    //             });
            
    //         // Find the position of our row after sorting
    //         for (int r = 0; r < (int)l1Block.size(); r++) {
    //             if (l1Block[r][0] == key) {
    //                 l1RowOff = r;
    //                 break;
    //             }
    //         }
    //     }

    //     // Step 2: Try to find space in one of the existing L2 pages
    //     auto &l1row = l1Block[l1RowOff];
    //     bool inserted = false;
    //     int maxL2Rows = static_cast<int>(L2->maxRowsPerBlock);

    //     for (int slot = 1; slot < (int)l1row.size(); slot++) {
    //         int l2PageNo = l1row[slot];
    //         if (l2PageNo == -1) continue;

    //         Cursor l2Cursor = L2->getCursor();
    //         l2Cursor.nextPage(l2PageNo);
    //         vector<vector<int>> l2Block = l2Cursor.getNextBlock();

    //         if ((int)l2Block.size() < maxL2Rows) {
    //             l2Block.push_back({ newPage, newSlot });
    //             bufferManager.writePage(L2name, l2PageNo, l2Block, l2Block.size(), 7, 0, L2name, l2Block);
    //             L2->rowsPerBlockCount[l2PageNo] = l2Block.size();
    //             inserted = true;
    //             break;
    //         }
    //     }

    //     if (!inserted) {
    //         // Check if we still have a -1 slot to add a new L2 page
    //         int emptySlot = -1;
    //         for (int slot = 1; slot < (int)l1row.size(); slot++) {
    //             if (l1row[slot] == -1) {
    //                 emptySlot = slot;
    //                 break;
    //             }
    //         }

    //         if (emptySlot != -1) {
    //             // Allocate new L2 page
    //             int newL2Page = L2->blockCount;
    //             vector<vector<int>> newL2Block = { { newPage, newSlot } };
    //             bufferManager.writePage(L2name, newL2Page, newL2Block, newL2Block.size(), 7, 0, L2name, newL2Block);
    //             L2->rowsPerBlockCount.push_back(1);
    //             L2->blockCount++;
    //             l1row[emptySlot] = newL2Page;
    //         } else {
    //             // Index is full, delete L1 and L2
    //             logger.log("Index on " + colName + " dropped due to lack of space.");
    //             tableCatalogue.deleteTable(L1name);
    //             tableCatalogue.deleteTable(L2name);
    //             it = table->indexcolumns.erase(it); // remove from indexcolumns
    //             continue;
    //         }
    //     }

    //     // Write updated L1 row
    //     bufferManager.writePage(L1name, l1BlockIdx, l1Block, l1Block.size(), 7, 0, L1name, l1Block);
    //     ++it;
    // }

    // table->rowCount++;