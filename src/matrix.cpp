#include "global.h"

// Constructors
Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
    this->blockCount = 0;
}

Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->matrixName = matrixName;
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->blockCount = 0;
    this->columnCount = 15;
    this->rowCount = 15;
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    this->maxColsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->rowCount));
}

Matrix::Matrix(string matrixName, int rows, int cols, vector<string> columns)
{
    logger.log("Matrix::Matrix");
    this->matrixName = matrixName;
    this->sourceFileName = "../data/temp/" + matrixName + ".csv";
    this->matrixColumnCount = rows; // we do not need matrixrowcount as it is square matrix
    this->blockCount = 0;
    this->columnCount = 15;
    this->rowCount = 15;
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    this->writeRow<string>(columns);
    this->maxColsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->rowCount));
}

// The load method – first reads the source file to determine the number of columns,
// then calls blockifyMatrix() to tile the matrix.
bool Matrix::load()
{
    logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        stringstream ss(line);
        string word;
        int columncounter = 0;
        while (getline(ss, word, ','))
            columncounter++;

        this->matrixColumnCount = columncounter;
        // this->columnCount = columncounter;
        // cout << columncounter;
        // Proceed only if we have a valid column count.
        if (this->matrixColumnCount > 0 && this->blockifyMatrix())
            return true;
    }
    return false;
}

// The writeBlock function merges multiple blocks (tiles) horizontally to form a 15-row segment.
// It uses matrixColumnCount to print only valid columns.
void Matrix::writeBlock(vector<vector<vector<int>>> &blocks, ostream &out, int matrixColumnCount)
{
    int blockSize = 15;                                                 // Each block is 15x15
    int blocksPerRow = (matrixColumnCount + blockSize - 1) / blockSize; // Number of blocks needed to form one full row

    // Iterate over each row within a 15-row block segment
    for (int row = 0; row < blockSize; row++)
    {

        // Print a single output row by merging corresponding rows from each block
        for (int blockIdx = 0; blockIdx < blocksPerRow; blockIdx++)
        {
            if (blockIdx < blocks.size()) // Ensure the block exists
            {
                // cout << blocks.size() << endl;
                for (int col = 0; col < blockSize; col++)
                {
                    // Only print valid matrix columns
                    if (col + blockIdx * blockSize < matrixColumnCount)
                        out << blocks[blockIdx][row][col] << " ";
                }
            }
        }

        out << endl; // End the current printed row
    }
}

// This method reads the source CSV file and stores the data as 15x15 blocks using the BufferManager.

// issue is the rowsperblockcount is not being set properly.
bool Matrix::blockifyMatrix()
{
    logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    const int blockSize = this->maxRowsPerBlock;
    // vector<vector<int>> blockBuffer(blockSize, vector<int>(blockSize, 0)); // 15×15 buffer

    int rowIdx = 0;
    int colIdx = 0;
    this->rowsPerBlockCount.clear();
    this->rowCount = 0;
    while (true)
    {
        vector<vector<int>> rowBuffer;
        int maxCols = 0;
        for (int i = 0; i < blockSize; ++i)
        {
            if (!getline(fin, line))
                break;
            stringstream s(line);
            vector<int> row;

            while (getline(s, word, ','))
                row.push_back(stoi(word));

            maxCols = max(maxCols, (int)row.size());
            rowBuffer.push_back(row);
        }
        int rowsRead = rowBuffer.size();
        // cout << "k" << endl;
        // cout << rowsRead << endl;
        this->rowCount += rowsRead;
        if (rowsRead == 0)
            break;
        for (int colStart = 0; colStart < maxCols; colStart += blockSize)
        {
            int colsRead = 0;
            vector<vector<int>> currentTile(blockSize, vector<int>(blockSize, 0));
            for (int r = 0; r < rowsRead; r++)
            {
                for (int c = 0; c < blockSize && (colStart + c) < rowBuffer[r].size(); c++)
                {
                    currentTile[r][c] = rowBuffer[r][colStart + c];
                    colsRead = c + 1;
                }
            }
            this->rowsPerBlockCount.push_back(rowsRead);
            this->colsPerBlockCount.push_back(colsRead);
            this->columnCount = colsRead;
            this->rows = rowIdx;
            this->cols = colIdx;
            bufferManager.writePage(this->matrixName, this->blockCount, currentTile, blockSize, 0, 0, this->matrixName, currentTile);
            this->blockCount++;
            colIdx += 1;
        }

        rowIdx += 1;
        colIdx = 0;
    }
    this->rows += 1;
    this->cols += 1;
    // cout << "FINAL ROW COUNT IN BLOCKIFY   " << this->rowCount << endl;
    // cout << this->rows << "   " << endl;
    // cout << this->cols << "    " << endl;
    // cout << "-------------------";
    // no of pages/blocks that the matrix is stored in
    // cout << this->blockCount << ":blockCOunt" << endl;

    // for (int i = 0; i < this->rowsPerBlock.size(); i++)
    //     cout << this->rowsPerBlock[i];
    // cout << endl;

    // // no of columns in the entire matrix
    // cout << this->matrixColumnCount << ":matrixcolcount" << endl;

    // // no of rows in the entire matrix
    // cout << this->rowCount << ":rowcount" << endl;
    // // no of rows in each block

    // cout << "Rows per block count: " << endl;
    // for (int i = 0; i < this->rowsPerBlockCount.size(); i++)
    //     cout << this->rowsPerBlockCount[i] << " ";
    // cout << endl;

    // cout << "Cols per block count: " << endl;
    // for (int i = 0; i < this->colsPerBlockCount.size(); i++)
    //     cout << this->colsPerBlockCount[i] << " ";
    // cout << endl;

    // cout << this->rows << ":rows" << endl;

    // cout << this->cols << ":cols" << endl;
    // cout << endl;
    // //
    // cout << this->columnCount << ":colcount" << endl;

    return (this->blockCount > 0);
}

// The print method uses a CursorMatrix to retrieve blocks one-by-one and
// prints the entire matrix in the correct order by merging blocks horizontally.

void Matrix::swapBlocks(int index1, int index2)
{
    Cursor cursor1(this->matrixName, 0);
    cursor1.nextPage(index1);
    vector<vector<int>> block_index1 = cursor1.getNextBlock();
    cursor1.nextPage(index2);
    vector<vector<int>> block_index2 = cursor1.getNextBlock();

    // page at index 1 gets replaced by page at index 2
    bufferManager.writePage(this->matrixName, index1, block_index2, this->maxRowsPerBlock, 2, index2, this->matrixName, block_index2);
    bufferManager.writePage(this->matrixName, index2, block_index1, this->maxRowsPerBlock, 2, index1, this->matrixName, block_index1);

    int temprows = this->rowsPerBlockCount[index1];
    int tempcols = this->colsPerBlockCount[index1];

    this->rowsPerBlockCount[index1] = this->rowsPerBlockCount[index2];
    this->colsPerBlockCount[index1] = this->colsPerBlockCount[index2];

    this->rowsPerBlockCount[index2] = temprows;
    this->colsPerBlockCount[index2] = tempcols;
}
void Matrix::TransposeMatrix()
{

    for (int i = 0; i < this->rows; i++)
    {
        for (int j = i + 1; j < this->cols; j++)
        {
            int ind1 = (i * this->cols) + j;
            int ind2 = (j * this->rows) + i;
            this->TransposeBlocks(ind1, ind2);
        }
        int ind = (i * this->rows) + i;
        this->TransposeBlocks(ind, ind);
    }
}
// this needs to be called on B and we are loading A
bool Matrix::CheckAntiSym(string matrixName1)
{
    Matrix *A = matrixCatalogue.getMatrix(matrixName1);

    if (A->rows != this->rows || A->cols != this->cols)
    {
        return false;
        // cout << "1" <<endl;
    }

    for (int i = 0; i < A->blockCount; i++)
    {
        if (A->rowsPerBlockCount[i] != this->rowsPerBlockCount[i])
        {
            //   cout << "2" <<endl;
            return false;
        }
    }

    for (int j = 0; j < A->blockCount; j++)
    {
        if (A->colsPerBlockCount[j] != this->colsPerBlockCount[j])
        {
            // cout << "3" <<endl;
            return false;
        }
    }

    // for B

    Cursor cursorB(this->matrixName, 0);
    cursorB.nextPage(0);
    // for A
    Cursor cursorA(matrixName1, 0);
    cursorA.nextPage(0);
    vector<vector<int>> blockB;
    vector<vector<int>> blockA;

    uint maxRowCount = this->maxRowsPerBlock;
    uint maxColCount = this->maxColsPerBlock;
    vector<int> row(maxColCount, 0);
    blockB.assign(maxRowCount, row);
    blockA.assign(maxRowCount, row);
    for (int i = 0; i < this->rows; i++)
    {
        for (int j = i + 1; j < this->cols; j++)
        {
            int ind1 = (i * this->cols) + j;
            int ind2 = (j * this->rows) + i;
            cursorB.nextPage(ind1);
            blockB = cursorB.getNextBlock();
            cursorA.nextPage(ind2);
            blockA = cursorA.getNextBlock();

            for (int k = 0; k < blockB.size(); k++)
            {
                for (int l = 0; l < blockB[0].size(); l++)
                {
                    if (blockB[k][l] != -blockA[l][k])
                        return false;
                }
            }
        }

        int ind = (i * this->rows) + i;

        cursorB.nextPage(ind);
        blockB = cursorB.getNextBlock();
        cursorA.nextPage(ind);
        blockA = cursorA.getNextBlock();
        // cout<<blockA.size()<<endl;
        // cout<<blockA[0].size()<<endl;
        // cout<<blockB.size()<<endl;
        // cout<<blockB[0].size()<<endl;
        // cout<<endl;
        for (int k = 0; k < blockB.size(); k++)
        {
            for (int l = 0; l < blockB[0].size(); l++)
            {
                if (blockB[k][l] != -blockA[l][k])
                {
                    // cout<<k<<"   "<<l<<endl;
                    return false;
                }
            }
        }
    }
    return true;
}
void Matrix::TransposeBlocks(int index1, int index2)
{
    Cursor cursor1(this->matrixName, 0);
    cursor1.nextPage(index1);
    vector<vector<int>> block_index1 = cursor1.getNextBlock();

    Cursor cursor2(this->matrixName, 0);
    cursor2.nextPage(index2);
    vector<vector<int>> block_index2 = cursor2.getNextBlock();

    // page at index 1 gets replaced by page at index 2
    // cout << "IN TRANSPOSE BLOCKS" << endl;
    // cout << this->matrixName << endl;
    bufferManager.writePage(this->matrixName, index1, block_index2, this->maxRowsPerBlock, 3, index2, this->matrixName, block_index2);
    // cout << "IN TRANSPOSE BLOCKS 2" << endl;
    // cout << this->matrixName << endl;
    bufferManager.writePage(this->matrixName, index2, block_index1, this->maxRowsPerBlock, 3, index1, this->matrixName, block_index1);

    // for(int i=0;i<block_index1.size();i++)
    // {
    //     for(int j=0;j<block_index1[i].size();j++)
    //     {
    //         cout<<block_index1[i][j]<<"  ";
    //     }
    //     cout<<endl;
    // }
    if (index1 == index2)
    {
        // cout << "Hello" << endl;
        int temprows = this->colsPerBlockCount[index1];
        int tempcols = this->rowsPerBlockCount[index1];
        // cout<<temprows<<"   "<<endl;
        // cout<<tempcols<<"   "<<endl;
        this->rowsPerBlockCount[index2] = temprows;
        this->colsPerBlockCount[index2] = tempcols;
    }
    else
    {
        int temprows = this->colsPerBlockCount[index1];
        int tempcols = this->rowsPerBlockCount[index1];

        this->rowsPerBlockCount[index1] = this->colsPerBlockCount[index2];
        this->colsPerBlockCount[index1] = this->rowsPerBlockCount[index2];

        this->rowsPerBlockCount[index2] = temprows;
        this->colsPerBlockCount[index2] = tempcols;
    }
}

// cout << "Matrix A row array: " << endl;
// for(int i = 0; i< A->rowsPerBlockCount.size();i++){
//     cout << A->rowsPerBlockCount[i] << " ";
// }
// cout << endl;

// cout << "Matrix A col array: " << endl;
// for(int i = 0; i< A->colsPerBlockCount.size();i++){
//     cout << A->colsPerBlockCount[i] << " ";
// }
// cout << endl;

void Matrix::crossTranspose(string matrixName1)
{
    Matrix *A = matrixCatalogue.getMatrix(matrixName1);

    Cursor this1 = Cursor(this->matrixName, 0);
    Cursor A1 = Cursor(A->matrixName, 0);

    uint loopBlockCount = min(A->blockCount, this->blockCount);

    string maxloopblock;
    string blocktowriteto;

    uint maxloopblockcount = max(A->blockCount, this->blockCount);
    if (A->blockCount > this->blockCount)
    {
        maxloopblock = A->matrixName;
        blocktowriteto = this->matrixName;
    }
    else
    {
        maxloopblock = this->matrixName;
        blocktowriteto = A->matrixName;
    }

    for (int i = 0; i < loopBlockCount; i++)
    {
        this1.nextPage(i);
        vector<vector<int>> thisblock = this1.getNextBlock();

        A1.nextPage(i);
        vector<vector<int>> A1block = A1.getNextBlock();
        // for crosstranspose A B
        // A is this, B is A1
        // this is writing B block into A
        bufferManager.writePage(this->matrixName, i, thisblock, A->maxRowsPerBlock, 4, i, A->matrixName, A1block);

        int temprows = this->rowsPerBlockCount[i];
        int tempcols = this->colsPerBlockCount[i];
        int tempmatrixcolcount = this->colsPerBlockCount[i];
        int temprowcount = this->rowsPerBlockCount[i];

        this->rowsPerBlockCount[i] = A->rowsPerBlockCount[i];
        this->colsPerBlockCount[i] = A->colsPerBlockCount[i];
        // this->matrixColumnCount = A->colsPerBlockCount[i];
        // this->rowCount = A->rowsPerBlockCount[i];

        // this puts A block into B
        bufferManager.writePage(A->matrixName, i, A1block, this->maxRowsPerBlock, 4, i, this->matrixName, thisblock);
        A->rowsPerBlockCount[i] = temprows;
        A->colsPerBlockCount[i] = tempcols;
        // A->matrixColumnCount = tempmatrixcolcount;
        // A->rowCount = temprowcount;
    }
    Cursor this2 = Cursor(maxloopblock, 0);
    Matrix *M = matrixCatalogue.getMatrix(blocktowriteto);
    for (int i = loopBlockCount; i < maxloopblockcount; i++)
    {
        this2.nextPage(i);
        vector<vector<int>> thisblock = this2.getNextBlock();
        bufferManager.writePage(blocktowriteto, i, thisblock, thisblock.size(), 0, i, A->matrixName, thisblock);
        bufferManager.deleteFile(maxloopblock, i);
        M->rowsPerBlockCount.push_back(thisblock.size());
        M->colsPerBlockCount.push_back(thisblock[0].size());
        M->blockCount++;
    }
    Matrix *M1 = matrixCatalogue.getMatrix(maxloopblock);
    // for(int i=loopBlockCount;i<maxloopblockcount;i++)
    // {
    //     M1->rowsPerBlockCount.pop_back();
    //     M1->colsPerBlockCount.pop_back();
    // }

    // vector<string> temp3 = this->columns;
    // this->columns.clear();
    // this->columns.assign(A->columns.begin(), A->columns.end());
    // A->columns.clear();
    // A->columns.assign(temp3.begin(), temp3.end());

    // long long int temp4 = this->columnCount;
    // this->columnCount = A->columnCount;
    // A->columnCount = temp4;

    long long int temp5 = M1->rowCount;
    M1->rowCount = M->rowCount;
    M->rowCount = temp5;

    int temp6 = M1->rows;
    M1->rows = M->rows;
    M->rows = temp6;

    int temp7 = M1->cols;
    M1->cols = M->cols;
    M->cols = temp7;

    uint temp8 = M->blockCount;
    M->blockCount = M1->blockCount;
    M1->blockCount = temp8;

    // vector<int> temp9 = this->rowsPerBlock;
    // this->rowsPerBlock.clear();
    // this->rowsPerBlock.assign(A->rowsPerBlock.begin(), A->rowsPerBlock.end());
    // A->rowsPerBlock.clear();
    // A->rowsPerBlock.assign(temp9.begin(), temp9.end());

    int temp10 = M->matrixColumnCount;
    M->matrixColumnCount = M1->matrixColumnCount;
    M1->matrixColumnCount = temp10;

    // int temp11 = this->maxRowsPerBlock;
    // this->maxRowsPerBlock = A->maxRowsPerBlock;
    // A->maxRowsPerBlock = temp11;

    // int temp12 = this->maxColsPerBlock;
    // this->maxColsPerBlock = A->maxColsPerBlock;
    // A->maxColsPerBlock = temp12;

    // vector<int> temp13 = this->rowsPerBlockCount;
    // this->rowsPerBlockCount.clear();
    // this->rowsPerBlockCount.assign(A->rowsPerBlockCount.begin(), A->rowsPerBlockCount.end());
    // A->rowsPerBlockCount.clear();
    // A->rowsPerBlockCount.assign(temp13.begin(), temp13.end());

    // vector<int> temp14 = this->colsPerBlockCount;
    // this->colsPerBlockCount.clear();
    // this->colsPerBlockCount.assign(A->colsPerBlockCount.begin(), A->colsPerBlockCount.end());
    // A->colsPerBlockCount.clear();
    // A->colsPerBlockCount.assign(temp14.begin(), temp14.end());
}
void Matrix::RotateMatrix(string matrixName)
{
    for (int i = 0; i < this->rows; i++)
    {
        for (int j = i + 1; j < this->cols; j++)
        {
            int ind1 = (i * this->cols) + j;
            int ind2 = (j * this->rows) + i;
            this->RotateBlocks(ind1, ind2);
        }
        // cout << "LOL     " << i  <<endl;
        int ind = (i * this->rows) + i;
        // cout << (i * this->rows) + i << endl;
        this->RotateBlocks(ind, ind);
    }

    for (int i = 0; i < this->rows; i++)
    {
        for (int j = 0; j < this->cols/2; j++)
        {
            int ind1 = (i * this->cols) + j;
            int ind2 = (i * this->rows) + this->cols - 1 - j;
            this->swapBlocks(ind1, ind2);
        }
    }
    return;
}

void Matrix::RotateBlocks(int index1, int index2)
{

    Cursor cursor1(this->matrixName, 0);
    cursor1.nextPage(index1);
    vector<vector<int>> block_index1 = cursor1.getNextBlock();
    Cursor cursor2(this->matrixName, 0);
    cursor2.nextPage(index2);
    vector<vector<int>> block_index2 = cursor2.getNextBlock();

    // page at index 1 gets replaced by page at index 2
    // cout << "IN TRANSPOSE BLOCKS" << endl;
    // cout << this->matrixName << endl;
    bufferManager.writePage(this->matrixName, index1, block_index2, this->maxRowsPerBlock, 5, index2,this->matrixName,block_index2);
    // cout << "IN TRANSPOSE BLOCKS 2" << endl;
    // cout << this->matrixName << endl;
    bufferManager.writePage(this->matrixName, index2, block_index1, this->maxRowsPerBlock, 5, index1,this->matrixName,block_index1);

    // for(int i=0;i<block_index1.size();i++)
    // {
    //     for(int j=0;j<block_index1[i].size();j++)
    //     {
    //         cout<<block_index1[i][j]<<"  ";
    //     }
    //     cout<<endl;
    // }
    if (index1 == index2)
    {
        // cout << "Hello" << endl;
        int temprows = this->colsPerBlockCount[index1];
        int tempcols = this->rowsPerBlockCount[index1];
        // cout<<temprows<<"   "<<endl;
        // cout<<tempcols<<"   "<<endl;
        this->rowsPerBlockCount[index2] = temprows;
        this->colsPerBlockCount[index2] = tempcols;
    }
    else
    {
        int temprows = this->colsPerBlockCount[index1];
        int tempcols = this->rowsPerBlockCount[index1];

        this->rowsPerBlockCount[index1] = this->colsPerBlockCount[index2];
        this->colsPerBlockCount[index1] = this->rowsPerBlockCount[index2];

        this->rowsPerBlockCount[index2] = temprows;
        this->colsPerBlockCount[index2] = tempcols;
    }
}
void Matrix::print()
{
    logger.log("Matrix::printMatrix");
    Cursor cursor(this->matrixName, 0);
    int printedRows = 0;
    // cout << "matrixcolumncount: " << this->matrixName << " "<< this->rowCount<< this->columnCount<<endl;
    if (this->matrixColumnCount < 17)
    {
        int rowsInBlock = this->matrixColumnCount;
        int colsInBlock = this->matrixColumnCount;
        // int rowsInBlock = 16;
        // int colsInBlock = 16;

        vector<vector<int>> block1 = cursor.getNextBlock();

        for (int r = 0; r < rowsInBlock; ++r)
        {
            for (int c = 0; c < colsInBlock; ++c)
            {
                std::cout << block1[r][c] << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
    else
    {

        int rowsInBlock = 0;
        int colsInBlock = 0;

        vector<vector<int>> block1 = cursor.getNextBlock();
        vector<vector<int>> block2 = cursor.getNextBlock();

        if (this->matrixColumnCount < 20)
        {
            rowsInBlock = this->matrixColumnCount;
            colsInBlock = this->matrixColumnCount;
        }
        else
        {
            rowsInBlock = 20;
            colsInBlock = 20;
        }

        for (int r = 0; r < 16; ++r)
        {
            for (int c = 0; c < colsInBlock; ++c)
            {
                if (c < block1[0].size())
                    std::cout << block1[r][c] << " ";
                else
                    std::cout << block2[r][c - block1[0].size()] << " ";
            }
            cout << endl;
        }
        Cursor cursor1(this->matrixName, 0);
        cursor1.nextPage(this->rows);
        block1= cursor1.getNextBlock();
        block2 = cursor1.getNextBlock();

        for (int r = 0; r < rowsInBlock - 16; ++r)
        {
            for (int c = 0; c < colsInBlock; ++c)
            {
                if (c < block1[0].size())
                    std::cout << block1[r][c] << " ";
                else
                    std::cout << block2[r][c - block1[0].size()] << " ";
            }
            cout << endl;
        }
    }
    printRowCount(this->rowCount);
}
// void Matrix::print()
// {
//     logger.log("Matrix::printMatrix");
//     Cursor cursor(this->matrixName, 0);
//     int printedRows = 0;
//     for (int i = 0; i < this->rows * this->cols; ++i)
//     {
//         int rowsInBlock = this->rowsPerBlockCount[i];
//         int printedCols = 0;
//         int j = i;
//         int colsInBlock = this->colsPerBlockCount[j];
//         vector<vector<int>> block = cursor.getNextBlock();
//         for (int r = 0; r < rowsInBlock; ++r)
//         {
//             for (int c = 0; c < colsInBlock; ++c)
//             {
//                 std::cout << block[r][c] << " ";
//             }
//             cout << endl;
//         }
//         printedCols += this->colsPerBlockCount[j];

//         printedRows += this->rowsPerBlockCount[i];
//     }

//     printRowCount(this->rowCount);
// }
// Called by CursorMatrix to load the next page from storage.
void Matrix::getNextPage(Cursor *cursor)
{
    // cout << "x"<<endl;
    logger.log("Matrix::getNextPage");
    if (cursor->pageIndex < this->blockCount - 1)
        cursor->nextPage(cursor->pageIndex + 1);
}

// Unloads the matrix pages and (if temporary) the source file.
void Matrix::unload()
{
    logger.log("Matrix::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
    {
        bufferManager.deleteFile(this->matrixName, pageCounter);
    }
    if (!isPermanent())
    {
        bufferManager.deleteFile(this->sourceFileName);
    }
}

// Returns whether the matrix source file is permanent.
bool Matrix::isPermanent()
{
    logger.log("Matrix::isPermanent");
    return (this->sourceFileName == "../data/" + this->matrixName + ".csv");
}
void Matrix::makePermanent() {
    logger.log("Matrix::makePermanent");
    
    if (!this->isPermanent()) {
        bufferManager.deleteFile(this->sourceFileName);
    }
    
    string newSourceFile = "../data/" + this->matrixName +".csv";
    ofstream fout(newSourceFile, ios::out);
    
    Cursor cursor(this->matrixName, 0);
    
    // Process matrix row by row
    for (int blockRow = 0; blockRow < this->rows; blockRow++) {
        int maxRowsInThisBlockRow = this->rowsPerBlockCount[blockRow * this->cols];
        
        // For each actual row in the blocks
        for (int r = 0; r < maxRowsInThisBlockRow; r++) {
            // For each pair of blocks in this row
            for (int blockCol = 0; blockCol < this->cols; blockCol++) {
                int blockIndex = blockRow * this->cols + blockCol;
                
                // Get current block
                cursor.nextPage(blockIndex);
                vector<vector<int>> currentBlock = cursor.getNextBlock();
                int colsInThisBlock = this->colsPerBlockCount[blockIndex];
                
                // Write the row from this block
                for (int c = 0; c < colsInThisBlock; c++) {
                    fout << currentBlock[r][c];
                    // Add comma if not at the end of full matrix row
                    if (!(blockCol == this->cols - 1 && c == colsInThisBlock - 1)) {
                        fout << ",";
                    }
                }
            }
            fout << endl;
        }
    }
    
    fout.close();
}
// void Matrix::makePermanent() {
//     logger.log("Matrix::makePermanent");
    
//     if (!this->isPermanent()) {
//         bufferManager.deleteFile(this->sourceFileName);
//     }
    
//     string newSourceFile = "../data/" + this->matrixName + ".csv";
//     ofstream fout(newSourceFile, ios::out);
    
//     Cursor cursor(this->matrixName, 0);
    
//     // Process matrix row by row
//     for (int blockRow = 0; blockRow < this->rows; blockRow++) {
//         // For each row of blocks, we need to process them row by row
//         vector<vector<vector<int>>> rowBlocks;
        
//         // First, gather all blocks in this row
//         for (int blockCol = 0; blockCol < this->cols; blockCol++) {
//             int blockIndex = blockRow * this->cols + blockCol;
//             cursor.nextPage(blockIndex);
//             rowBlocks.push_back(cursor.getNextBlock());
//         }
        
//         // Now write each row from these blocks side by side
//         int maxRowsInThisBlockRow = this->rowsPerBlockCount[blockRow * this->cols];
        
//         // Process each row within the blocks
//         for (int r = 0; r < maxRowsInThisBlockRow; r++) {
//             // For each row, go through all blocks in this block-row
//             for (int blockCol = 0; blockCol < this->cols; blockCol++) {
//                 int blockIndex = blockRow * this->cols + blockCol;
//                 int colsInThisBlock = this->colsPerBlockCount[blockIndex];
                
//                 // Write the row from this block
//                 for (int c = 0; c < colsInThisBlock; c++) {
//                     fout << rowBlocks[blockCol][r][c];
//                     // Add comma if not at the end of full matrix row
//                     if (!(blockCol == this->cols - 1 && c == colsInThisBlock - 1)) {
//                         fout << ",";
//                     }
//                 }
//             }
//             fout << endl;
//         }
//     }
    
//     fout.close();
// }

// void Matrix::makePermanent()
// {
//     logger.log("Matrix::makePermanent");
//     if (!this->isPermanent())
//         bufferManager.deleteFile(this->sourceFileName);
//     string newSourceFile = "../data/" + this->matrixName + ".csv";
//     ofstream fout(newSourceFile, ios::out);
//     Cursor cursor(this->matrixName, 0);
//     vector<int> row;
//     for (int i = 0; i < this->rows; ++i)
//     {
//         for (int j = 0; j < this->cols; ++j)
//         {
//             int rowsInBlock = this->rowsPerBlockCount[i];
//             int j = i;
//             int colsInBlock = this->colsPerBlockCount[j];
//             vector<vector<int>> block = cursor.getNextBlock();
//             for (int r = 0; r < rowsInBlock; ++r)
//             {
//                 for (int c = 0; c < colsInBlock; ++c)
//                 {
//                     fout << block[r][c] << " ";
//                 }
//                 fout << endl;
//             }
//         }
//     }
//     fout.close();
// }

// Returns a CursorMatrix for iterating over this matrix.
Cursor Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    return Cursor(this->matrixName, 0);
}