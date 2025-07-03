#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{

    this->pageName = "";
    this->tableName = "";
    this->pageIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName
 * @param pageIndex
 */
Page::Page(string tableName, int pageIndex)
{
    // cout<<"ENTERED PAGE FUNCTION"<<endl;
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    // cout<<"******    " <<this->pageName<<endl;
    Table *checkingTable = tableCatalogue.getTable(tableName);
    if (checkingTable == nullptr)
    {
        Matrix matrix = *matrixCatalogue.getMatrix(tableName);
        uint maxRowCount = matrix.maxRowsPerBlock;
        uint maxColCount = matrix.maxColsPerBlock;
        ifstream fin(pageName, ios::in);
        this->rowCount = matrix.rowsPerBlockCount[pageIndex];
        this->columnCount = matrix.colsPerBlockCount[pageIndex];
        vector<int> row(this->columnCount, 0);
        this->rows.assign(this->rowCount, row);
        int number;
        for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        {
            for (uint columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                fin >> number;
                this->rows[rowCounter][columnCounter] = number;
            }
            std::string remainingLine;
            std::getline(fin, remainingLine);
        }

        fin.close();
        return;
    }
    else
    {
        Table table = *tableCatalogue.getTable(tableName);

        this->columnCount = table.columnCount;
        uint maxRowCount = table.maxRowsPerBlock;
        vector<int> row(columnCount, 0);
        this->rows.assign(maxRowCount, row);
        // cout<<pageName<<endl;
        ifstream fin(pageName, ios::in);
        // cout << "HELLO"<<endl;
        this->rowCount = table.rowsPerBlockCount[pageIndex];
        // cout <<"***************"<<endl;
        // cout << table.rowsPerBlockCount[pageIndex] <<endl;
        // cout << pageIndex << endl;
        // cout << this->rowCount<<endl;
        // cout << this->columnCount<<endl;
        int number;
        // cout << "LLLLL"<<endl;

        for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        {
            for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
            {
                // cout << rowCounter <<endl;
                // cout<<columnCounter<<endl;
                fin >> number;
                this->rows[rowCounter][columnCounter] = number;
            }
            // cout << "Hi"<<endl;
        }
        // cout << "0"<<endl;
        fin.close();
    }
}

/**
 * @brief Get row from page indexed by rowIndex
 *
 * @param rowIndex
 * @return vector<int>
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    if (rowIndex >= this->rowCount)
        return result;
    // cout<<"The row index is  " << rowIndex;
    // for(int i=0;i<rows[rowIndex].size();i++)
    // cout << this->rows[rowIndex][i] << " ";
    // cout <<endl;
    return this->rows[rowIndex];
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
}

/**
 * @brief writes current page contents to file.
 *
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    // cout << "IN PAGE " << endl;
    // cout << this->rowCount << endl;
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}

void Page::writeRowToPage(vector<int> rows1, int flag)
{
    logger.log("Page::writePage");
    this->rows.emplace_back(rows1);
    ofstream fout(this->pageName, ios::app);
    for (int rowCounter = 0; rowCounter < rows1.size(); rowCounter++)
    {
        fout << rows1[rowCounter] << " ";
    }
    fout << endl;
    fout.close();
}

void Page::clearPage()
{
        this->rows.clear();
    ofstream fout(this->pageName, ios::trunc);
    fout.close();

}

void Page::writeMatrixPage()
{
    // cout<<this->columnCount<<endl;
    // cout<<this->rowCount<<endl;
    // cout<<"Hello"<<endl;
    logger.log("MatrixPage::writeMatrixPage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}
void Page::writeSwapPage(string TableName, vector<vector<int>> rows, int pageIndex, int switchPageInd)
{

    Matrix *m = matrixCatalogue.getMatrix(TableName);
    this->rowCount = m->rowsPerBlockCount[switchPageInd];
    this->columnCount = m->colsPerBlockCount[switchPageInd];
    vector<int> row(m->maxColsPerBlock, 0);
    this->rows.assign(m->maxRowsPerBlock, row);
    logger.log("MatrixPage::writeMatrixPage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            this->rows[rowCounter][columnCounter] = rows[rowCounter][columnCounter];
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}

// given page of tableName1, we want to take rows2 from tablesName2 and swap it with rows1 from tableName1 in the page index pageIndex for both
// The current page (this->) is that of tableName1

// writing from table 2 to table 1
void Page::writeSwapDiffPage(string TableName1, string TableName2, vector<vector<int>> rows1, vector<vector<int>> rows2, int pageIndex, int switchPageInd)
{

    Matrix *m = matrixCatalogue.getMatrix(TableName1);
    Matrix *m1 = matrixCatalogue.getMatrix(TableName2);

    int temprowCount = this->rowCount;
    int tempcolCount = this->columnCount;

    // cout << "IN DIFF FUNCTION" << endl;
    // int maxrowsperblock=this->maxColsPerBlock;
    // int maxcolsperblock=this->maxRowsPerBlock;

    // i'm putting rows2 into the page of table1?
    this->rowCount = rows2.size();
    this->columnCount = rows2[0].size();

    vector<int> row(m1->maxColsPerBlock, 0);
    this->rows.assign(m1->maxRowsPerBlock, row);

    logger.log("MatrixPage::writeMatrixPage");
    ofstream fout(this->pageName, ios::trunc);
    // cout<<"******"<<endl;
    // cout << rows2.size() <<endl;
    // cout<< rows2[0].size() <<endl;
    // cout<<"()"<<endl;
    // cout<<this->rows.size()<<endl;
    // cout<<this->rows[0].size()<<endl;
    // cout<<"****************"<<endl;
    // cout<<this->rowCount<<endl;
    // cout<<this->columnCount<<endl;
    // cout<<"**********"<<endl;

    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            this->rows[rowCounter][columnCounter] = rows2[rowCounter][columnCounter];
            fout << this->rows[rowCounter][columnCounter];
            //  cout << this->rows[rowCounter][columnCounter] <<"  ";
        }
        // cout << endl;
        fout << endl;
    }

    // m->rowCount=temprowCount;
    // m->columnCount=tempcolCount;

    fout.close();
}

void Page::writeTransposedSwapPage(string tableName, vector<vector<int>> rows, int pageIndex, int switchPageInd)
{
    // cout<<"HELLI"<<endl;
    Matrix *m = matrixCatalogue.getMatrix(tableName);
    // cout<<"*****"<<endl;
    // cout<<this->rowCount<<endl;
    // cout<<this->columnCount<<endl;
    // cout<<"**************";
    this->rowCount = m->colsPerBlockCount[switchPageInd];
    this->columnCount = m->rowsPerBlockCount[switchPageInd];
    vector<int> row(m->maxRowsPerBlock, 0);
    this->rows.assign(m->maxColsPerBlock, row);
    logger.log("MatrixPage::writeMatrixPage");
    // cout<<this->pageName<<endl;
    ofstream fout(this->pageName, ios::trunc);

    // cout<<"*****"<<endl;
    // cout<<this->rowCount<<endl;
    // cout<<this->columnCount<<endl;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            this->rows[rowCounter][columnCounter] = rows[columnCounter][rowCounter];
            // cout << this->rows[rowCounter][columnCounter] <<"    ";
            fout << this->rows[rowCounter][columnCounter];
        }
        // cout<<endl;
        fout << endl;
    }
    // cout<<endl;
    // cout<<endl;
    // for(int i=0;i<this->rows.size();i++)
    // {
    //     for(int j=0;j<this->rows[0].size();j++)
    //     {
    //         cout<<this->rows[i][j]<<"   ";
    //     }
    //     cout<<endl;
    // }
    // cout<<"((((((((((((((((()))))))))))))))))"<<endl;
    // cout<<this->rowCount<<endl;
    // cout<<this->columnCount<<endl;
    // cout<<"00000000000000"<<endl;
    // for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    // {
    //     for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    //     {
    //         cout << this->rows[rowCounter][columnCounter] <<"    ";
    //     }
    //     cout<<endl;
    // }
    // cout<<"000000000000000";
    fout.close();
}

void Page::writeRotatedSwapPage1(string tableName, vector<vector<int>> rows, int pageIndex, int switchPageInd)
{
    Matrix *m = matrixCatalogue.getMatrix(tableName);
    this->rowCount = m->colsPerBlockCount[switchPageInd];
    this->columnCount = m->rowsPerBlockCount[switchPageInd];
    vector<int> row(m->maxRowsPerBlock, 0);
    this->rows.assign(m->maxColsPerBlock, row);

    logger.log("MatrixPage::writeRotatedMatrixPage");
    ofstream fout(this->pageName, ios::trunc);

    // First transpose the matrix
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            this->rows[rowCounter][columnCounter] = rows[columnCounter][rowCounter];
        }

        // Then swap columns for each row
        for (int columnCounter = 0; columnCounter < this->columnCount / 2; columnCounter++)
        {
            int temp = this->rows[rowCounter][columnCounter];
            this->rows[rowCounter][columnCounter] = this->rows[rowCounter][this->columnCount - 1 - columnCounter];
            this->rows[rowCounter][this->columnCount - 1 - columnCounter] = temp;
        }
    }

    // Write to file
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}