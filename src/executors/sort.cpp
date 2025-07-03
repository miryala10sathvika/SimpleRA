
#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 *
 * sorting_order = ASC | DESC
 */
vector<pair<int, bool>> columnSortingPairs;

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if (tokenizedQuery.size() < 6 || tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1];

    parsedQuery.sortColumns.clear();

    size_t i = 3;
    while (i < tokenizedQuery.size() && tokenizedQuery[i] != "IN")
    {
        parsedQuery.sortColumns.push_back(tokenizedQuery[i]);
        i++;
    }

    if (i + 1 >= tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.sortingStrategies.clear();
    for (size_t j = i + 1; j < tokenizedQuery.size(); j++)
    {
        if (tokenizedQuery[j] == "ASC")
            parsedQuery.sortingStrategies.push_back(ASC);
        else if (tokenizedQuery[j] == "DESC")
            parsedQuery.sortingStrategies.push_back(DESC);
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    if (parsedQuery.sortColumns.size() != parsedQuery.sortingStrategies.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    for (int i = 0; i < parsedQuery.sortColumns.size(); i++)
    {
        if (!tableCatalogue.isColumnFromTable(parsedQuery.sortColumns[i], parsedQuery.sortRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}
void sortMatrix(vector<vector<int>> &data, const vector<pair<int, bool>> &columns)
{
    sort(data.begin(), data.end(), [&](const vector<int> &row1, const vector<int> &row2)
         {
        for (const auto &[col, asc] : columns) {
            if (row1[col] != row2[col]) {
                return asc ? row1[col] < row2[col] : row1[col] > row2[col];
            }
        }
        return false; });
}

struct Compare
{
    bool operator()(const tuple<vector<int>, int, int,int> &a, const tuple<vector<int>, int, int,int> &b)
    {
        const vector<int> &row1 = get<0>(a);
        const vector<int> &row2 = get<0>(b);

        for (const auto &[col, asc] : columnSortingPairs)
        {
            if (row1[col] != row2[col])
            {
                return asc ? row1[col] > row2[col] : row1[col] < row2[col]; 
            }
        }
        return false;
    }
};





void mergeSortedBuffers(vector<vector<vector<int>>> &buffers, int k, int s, vector<pair<int, bool>> &columns,Table *table,int rowsWritten)
{

    priority_queue<tuple<vector<int>, int, int,int>, vector<tuple<vector<int>, int, int,int>>, Compare> minHeap;
    for (int i = 0; i < k; i++)
    {
        if (!buffers[i].empty())
        {
            minHeap.push({buffers[i][0], i, 0,0});
        }
    }
    while (!minHeap.empty())
    {
        auto [row, bufferIdx, rowIdx,u] = minHeap.top();
        minHeap.pop();

        if(rowsWritten==table->maxRowsPerBlock)
        {
            rowsWritten=1;
            s++;
            bufferManager.writePageRow(parsedQuery.sortRelationName, s, row, row.size(), 20, 0, "", buffers[0]);
        }
        else 
        {
            rowsWritten++;
            bufferManager.writePageRow(parsedQuery.sortRelationName, s, row, row.size(), 20, 0, "", buffers[0]);
        }
        if (rowIdx + 1 < buffers[bufferIdx].size())
        {
            minHeap.push({buffers[bufferIdx][rowIdx + 1], bufferIdx, rowIdx + 1,0});
        }
    }
}

void executeSORT()
{
    logger.log("executeSORT");

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    int i = 1;
    int j = table->blockCount;
    int k=10;

    int m = ceil(float(j) / (k));
    Cursor cursor = table->getCursor();
    vector<vector<vector<int>>> buffers(k);
    int currblocksread = 0;
    columnSortingPairs.clear();
    for (size_t o = 0; o < parsedQuery.sortColumns.size(); o++)
    {
        columnSortingPairs.emplace_back(table->getColumnIndex(parsedQuery.sortColumns[o]), !parsedQuery.sortingStrategies[o]);
    }

    vector<int> sublistStartIndices;
    vector<int> sublistEndIndices;
    while (i <= m)
    {
        int buffercount = 0;
        int temp = currblocksread;

        for (int blockno = 0; blockno < k && currblocksread < (table->blockCount); blockno++)
        {
            currblocksread++;
            buffers[blockno] = cursor.getNextBlock();
            buffercount++;
        }
        sublistStartIndices.push_back(currblocksread - buffercount);
        sublistEndIndices.push_back(currblocksread);
       
        for (int b = 0; b < buffercount; b++)
        {
            sortMatrix(buffers[b], columnSortingPairs);
        }
        int o = 0;
        
        mergeSortedBuffers(buffers,buffercount,temp,columnSortingPairs,table,0);
        i++;
    }
    bufferManager.clearSet();


    priority_queue<tuple<vector<int>, int, int,int>,
                   vector<tuple<vector<int>, int, int,int>>,
                   Compare>
        minHeap;

    i = 1;
    int p = ceil(log(m) / (log(k - 1)));
    j = m;

    vector<Cursor> cursors;
    buffers.assign(k, vector<vector<int>>());
    int outputBufferIndex = k - 1;

    for (int s = 0; s < m; s++)
    {
        Cursor sublistCursor = table->getCursor();
        sublistCursor.nextPage(sublistStartIndices[s]);
        cursors.push_back(sublistCursor);

        buffers[s] = sublistCursor.getNextBlock();
        if (!buffers[s].empty())
            {
                minHeap.push({buffers[s][0], s, 0,sublistStartIndices[s]});
            }
    }


    int pagetoWriteto=table->blockCount;
    while (i <= p)
    {
        int n = 1;
        int q = ceil(float(j) / (k - 1));
        

        while (!minHeap.empty())
        {
            auto [row, sublistIdx, rowIdx,currpageindex] = minHeap.top();
            
            minHeap.pop();
            buffers[outputBufferIndex].push_back(row);

            
            if (rowIdx + 1 < buffers[sublistIdx].size())
            {
                minHeap.push({buffers[sublistIdx][rowIdx + 1], sublistIdx, rowIdx + 1,currpageindex});
            }
            else  
            {
                
                if(sublistEndIndices[sublistIdx]<=currpageindex+1)
                {
                    // cout<<"return"<<endl;
                }
                else 
                {
                cursors[sublistIdx].nextPage(currpageindex+1);
                buffers[sublistIdx] = cursors[sublistIdx].getNextBlock();
                if (!buffers[sublistIdx].empty())
                    minHeap.push({buffers[sublistIdx][0], sublistIdx, 0,currpageindex+1});
                }

            }

            
         
            if (buffers[outputBufferIndex].size() == table->maxRowsPerBlock)
            {
               
                bufferManager.writePage(parsedQuery.sortRelationName, pagetoWriteto, buffers[outputBufferIndex], buffers[outputBufferIndex].size(), 6, 0, "", buffers[outputBufferIndex]);
                pagetoWriteto++;
                table->rowsPerBlockCount.emplace_back(buffers[outputBufferIndex].size());
                buffers[outputBufferIndex].clear();
            }
        }
        j = q;
        i++;
    }

    
    if (!buffers[outputBufferIndex].empty())
    {
        
        bufferManager.writePage(parsedQuery.sortRelationName, pagetoWriteto, buffers[outputBufferIndex], buffers[outputBufferIndex].size(), 6, 0, "", buffers[outputBufferIndex]);
        table->rowsPerBlockCount.emplace_back(buffers[outputBufferIndex].size());
        pagetoWriteto++;
    }

    
    for(int i=table->blockCount;i<pagetoWriteto;i++)
    {
        Cursor c= table->getCursor();
        c.nextPage(i);
       buffers[0]=c.getNextBlock();
       bufferManager.writePage(parsedQuery.sortRelationName, i-table->blockCount, buffers[0], buffers[0].size(), 6, 0, "", buffers[0]);
    }

    for(int i=table->blockCount;i<pagetoWriteto;i++)
    {
    bufferManager.deleteFile(parsedQuery.sortRelationName,i);
    }

    return;
}
