#include "global.h"

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the pag e is read and then inserted into the pool.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::getPage(string tableName, int pageIndex)
{

    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
    {
        // cout << "1"<<endl;
        return this->getFromPool(pageName);
    }
    else
    {
        // cout << "2"<<endl;
        return this->insertIntoPool(tableName, pageIndex);
    }
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName
 * @return true
 * @return false
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto page : this->pages)
    {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName
 * @return Page
 */
Page BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}


/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    // cout<<"11111"<<endl;

    Page page(tableName, pageIndex);
    // cout<<"22222"<<endl;
    if (this->pages.size() >= BLOCK_COUNT)
        pages.pop_front();
    pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 * @param rowCount
 */
unordered_set<string> clearedPages;
void BufferManager::writePageRow(string tableName, int pageIndex, vector<int> rows, int rowCount, int type, int switchPageInd, string tableName2, vector<vector<int>> rows2)
{
    if (type == 20)
    {
        string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
        if (inPool(pageName) == true)
        {
         
            Page page1 = getFromPool(pageName); 

            if (clearedPages.find(pageName) == clearedPages.end())
            {
                page1.clearPage(); 
                page1.writeRowToPage(rows, 1);
                clearedPages.insert(pageName);
            }
            else
            {
                page1.writeRowToPage(rows, 0);
            }
            
            if (inPool(pageName))
            {
                for (auto it = pages.begin(); it != pages.end(); ++it)
                {
                    if (it->pageName == pageName)
                    {
                        pages.erase(it);
                        break;
                    }
                }
                pages.push_back(page1);
            }
            return;
        }
        else
        {
            Page page(tableName, pageIndex, rows2, rowCount);
            if (clearedPages.find(pageName) == clearedPages.end())
            {
                page.clearPage(); 
                page.writeRowToPage(rows, 1);
                clearedPages.insert(pageName);
            }
            else
            {
                page.writeRowToPage(rows, 0);
            }

            if (inPool(pageName))
            {
                for (auto it = pages.begin(); it != pages.end(); ++it)
                {
                    if (it->pageName == pageName)
                    {
                        pages.erase(it);
                        break;
                    }
                }
                pages.push_back(page);
            }
            return;
        }
    }
}

void BufferManager::clearSet()
{
    clearedPages.clear();
    return;
}
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount, int type, int switchPageInd, string tableName2, vector<vector<int>> rows2)
{
    logger.log("BufferManager::writePage");
    // cout << "In buffermanager" << endl;
    // cout << pageIndex << "  " << rowCount   << endl;
    Page page(tableName, pageIndex, rows, rowCount);

    if (type == 0)
        page.writeMatrixPage();
    if (type == 2)
    {
        // cout << "HEREEEE123" << endl;
        page.writeSwapPage(tableName, rows, pageIndex, switchPageInd);
    }

    if (type == 3)
    {
        // cout<<"HERE??"<<endl;
        page.writeTransposedSwapPage(tableName, rows, pageIndex, switchPageInd);
    }
    if (type == 4)
    {
        page.writeSwapDiffPage(tableName, tableName2, rows, rows2, pageIndex, switchPageInd);
    }
    if (type == 5)
    {
        page.writeRotatedSwapPage1(tableName, rows, pageIndex, switchPageInd);
    }
    else
        page.writePage();
    // After writing, we should update the pool if this page is cached
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (inPool(pageName))
    {
        // Remove the old page from pool
        for (auto it = pages.begin(); it != pages.end(); ++it)
        {
            if (it->pageName == pageName)
            {
                pages.erase(it);
                break;
            }
        }
        // Insert the new page
        pages.push_back(page);
    }
}

/**
 * @brief Deletes file names fileName
 *
 * @param fileName
 */
void BufferManager::deleteFile(string fileName)
{

    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::deleteFile(string tableName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
    // cout << "In buffer manager" << endl;
    // for (const auto& page : this->pages) {
    //     cout << page.pageName << endl;
    // }
    // cout << endl;
}

void BufferManager::removePage(string tableName,int pageIndex)
{
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (inPool(pageName))
    {
        // Remove the old page from pool
        for (auto it = pages.begin(); it != pages.end(); ++it)
        {
            if (it->pageName == pageName)
            {
                pages.erase(it);
                break;
            }
        }
        // Insert the new page
        // pages.push_back(page);
    }
}


void BufferManager::clearCSV(string tableName)
{
    logger.log("BufferManager::clearCSV");
        string fileName = "../data/temp/" + tableName + ".csv";
    
    ifstream fin(fileName);  
    if (!fin.is_open()) {

        return;
    }

    string header;
    if (!getline(fin, header)) {  
        fin.close();
        return;
    }
    fin.close();
    ofstream fout(fileName, ios::trunc);
    if (!fout.is_open()) {
        return;
    }

    fout << header << endl;  
    fout.close(); 
}