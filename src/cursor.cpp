#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{

    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getNext()
{
    logger.log("Cursor::geNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty())
    {
        tableCatalogue.getTable(this->tableName)->getNextPage(this);
        if (!this->pagePointer)
        {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}

vector<vector<int>> Cursor::getNextBlock()
{
    logger.log("Cursor::getNextBlock");
    vector<vector<int>> block;
    // Get all rows in the current page starting from pagePointer
    while (true)
    {
        vector<int> row = this->page.getRow(this->pagePointer);
        // cout << row.size() << endl;
        // If we get an empty row
        if (row.empty())
        {
           
            // Try to get the next page
            Table *checkingTable = tableCatalogue.getTable(this->tableName);
            if (checkingTable == nullptr)
            {
                Matrix* matrix=matrixCatalogue.getMatrix(this->tableName);
                matrixCatalogue.getMatrix(this->tableName)->getNextPage(this);
            }
            else
            {
                tableCatalogue.getTable(this->tableName)->getNextPage(this);
            }
            // cout<<"*****"<<endl;
            // cout<<this->pagePointer <<endl;
            // cout<<this->pageIndex<<endl;
            // cout<<"******"<<endl;
            // If pagePointer is reset to 0, we have a new page
            if (!this->pagePointer && this->pageIndex==0 )
            {
                // cout << "Hello1" <<endl;
                row = this->page.getRow(this->pagePointer);

                if (!row.empty())
                {
                    // block.push_back(row);
                    this->pagePointer++;
                }
            }
            // If we couldn't get a new row after page change, return the block
            break;
        }
        // cout<<"******";
        // // Add the  row to our block and increment pointer
        // for(int i=0;i<row.size();i++)
        // {
        //     cout<<row[i]<<"   ";
        // }
        // cout<<endl;
        block.push_back(row);

        this->pagePointer++;
    }
    // cout<<block.size()<<endl;
    // cout<<block[0].size()<<endl;
    return block;
}
/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex
 */
void Cursor::nextPage(int pageIndex)
{
    logger.log("Cursor::nextPage");
    this->page = bufferManager.getPage(this->tableName, pageIndex);
    this->pageIndex = pageIndex;
    this->pagePointer = 0;
}

