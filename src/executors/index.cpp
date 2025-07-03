#include "global.h"
/**
 * @brief
 * SYNTAX: INDEX ON column_name FROM relation_name USING indexing_strategy
 * indexing_strategy: ASC | DESC | NOTHING
 */
bool syntacticParseINDEX()
{
    logger.log("syntacticParseINDEX");
    if (tokenizedQuery.size() != 7 || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "USING")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INDEX;
    parsedQuery.indexColumnName = tokenizedQuery[2];
    parsedQuery.indexRelationName = tokenizedQuery[4];
    string indexingStrategy = tokenizedQuery[6];
    if (indexingStrategy == "BTREE")
        parsedQuery.indexingStrategy = BTREE;
    else if (indexingStrategy == "HASH")
        parsedQuery.indexingStrategy = HASH;
    else if (indexingStrategy == "NOTHING")
        parsedQuery.indexingStrategy = NOTHING;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseINDEX()
{
    logger.log("semanticParseINDEX");
    if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (!tableCatalogue.isColumnFromTable(parsedQuery.indexColumnName, parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    if (table->indexed)
    {
        cout << "SEMANTIC ERROR: Table already indexed" << endl;
        return false;
    }
    return true;
}


void executeINDEX()
{
    map<int, list<pair<int, int>>> map1;
    map<int, list<int>> map2;
    Table *table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    Cursor c = table->getCursor();
    c.nextPage(0);
    vector<vector<int>> block;
    int index = table->getColumnIndex(parsedQuery.indexColumnName);
    table->indexcolumns.push_back(parsedQuery.indexColumnName);
    int rowL2 = 0;
    
    for (int i = 0; i < table->blockCount; i++)
    {
        c.nextPage(i);
        block = c.getNextBlock();
        for (int j = 0; j < block.size(); j++)
        {
            rowL2++;
            map1[block[j][index]].push_back({i, j});
        }
    }
    block.clear();
    

    vector<string> newColumns;
    newColumns.push_back("BlockNo");
    newColumns.push_back("RowNo");
    string name = parsedQuery.indexRelationName + parsedQuery.indexColumnName + "L2";
    Table *L2Table = new Table(name, newColumns);
    
    int maxRowsPerBlock70Percent = static_cast<int>(L2Table->maxRowsPerBlock * 0.7);
    
    int pageCount = 0;
    int rowL1 = 0;
    for (const auto &[key, pairList] : map1)
    {
        for (const auto &p : pairList)
        {
            block.push_back({p.first, p.second});
            vector<int> row100 = {p.first, p.second};
            L2Table->writeRow(row100);
            

            if (block.size() >= maxRowsPerBlock70Percent)
            {
                bufferManager.writePage(name, pageCount, block, block.size(), 7, 0, name, block);
                L2Table->rowsPerBlockCount.emplace_back(block.size());
                map2[key].push_back(pageCount);
                pageCount++;
                block.clear();
            }
        }
        

        if (block.size() > 0)
        {
            bufferManager.writePage(name, pageCount, block, block.size(), 7, 0, name, block);
            L2Table->rowsPerBlockCount.emplace_back(block.size());
            map2[key].push_back(pageCount);
            block.clear();
            pageCount++;
        }
    }
    
    tableCatalogue.insertTable(L2Table);
    L2Table->blockCount = pageCount;
    L2Table->rowCount = rowL2;
    

    size_t maxSize = 0;
    for (const auto &[key, lst] : map2)
    {
        rowL1++;
        maxSize = max(maxSize, lst.size());
    }
    
    for (auto &[key, lst] : map2)
    {
        while (lst.size() < maxSize)
        {
            lst.push_back(-1);
        }
    }
    
    string name1 = parsedQuery.indexRelationName + parsedQuery.indexColumnName + "L1";
    vector<string> newColumns1;
    newColumns1.push_back("FieldVal");
    for (int i = 0; i < maxSize; i++)
    {
        newColumns1.push_back(to_string(i));
    }
    
    Table *L1Table = new Table(name1, newColumns1);
    block.clear();
    pageCount = 0;
    

    int maxRowsPerBlockL1_70Percent = static_cast<int>(L1Table->maxRowsPerBlock * 0.7);
    
    for (const auto &[key, lst] : map2)
    {
        vector<int> res;
        res.push_back(key);
        for (auto elem : lst)
        {
            res.push_back(elem);
        }
        block.push_back(res);
        L1Table->writeRow(res);
        
 
        if (block.size() >= maxRowsPerBlockL1_70Percent )
        {
            bufferManager.writePage(name1, pageCount, block, block.size(), 7, 0, name1, block);
            L1Table->rowsPerBlockCount.emplace_back(block.size());
            pageCount++;
            block.clear();
        }
    }
    

    if (block.size() > 0)
    {
        bufferManager.writePage(name1, pageCount, block, block.size(), 7, 0, name1, block);
        L1Table->rowsPerBlockCount.emplace_back(block.size());
        pageCount++;
        block.clear();
    }
    
    tableCatalogue.insertTable(L1Table);
    L1Table->blockCount = pageCount;
    L1Table->rowCount = rowL1;
    
    logger.log("executeINDEX");
    return;
}