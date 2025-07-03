// #include "cursor.h"

class Matrix {
public:
    string sourceFileName;
    string matrixName;
    vector<string> columns;
    long long int columnCount = 0;
    long long int rowCount = 0;
    int rows;
    int cols;
    uint blockCount = 0;
    vector<int> rowsPerBlock;
    bool blockifyMatrix();
    int matrixColumnCount;
    int maxRowsPerBlock = 0;
    int maxColsPerBlock=0;
    vector<int> rowsPerBlockCount;
    vector <int> colsPerBlockCount;
    bool indexed = false;
    string indexedColumn = "";


public:
    Matrix();
    Matrix(string matrixName);
    Matrix(string matrixName, int rows, int cols,vector<string> columns);
    bool load();
    void unload();
    bool store(vector<vector<int>>& data);
    vector<vector<int>> retrieve();
    bool isPermanent();
    void makePermanent();
    string getMatrixName() { return matrixName; }
    void print();
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    void writeBlock(vector<vector<vector<int>>> &blocks, ostream &out, int matrixColumnCount);
    void swapBlocks(int index1,int index2);
    void TransposeBlocks(int index1,int index2);
    void TransposeMatrix();
    void crossTranspose(string matrixName1);
    bool CheckAntiSym(string matrixName1);
    void RotateMatrix(string matrixName);
    void RotateBlocks(int index1, int index2);
    template <typename T>

void writeRow(vector<T> row, ostream &fout)
{
    logger.log("Matrix::printRow");
    int columnsToWrite = min(this->matrixColumnCount, static_cast<int>(row.size()));
    for (int columnCounter = 0; columnCounter < columnsToWrite; columnCounter++)
    {
        if (columnCounter != 0)
            fout << ", "; 
        fout << row[columnCounter]; 
    }
    
    fout << endl;  
}

/**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row 
 */
template <typename T>
void writeRow(vector<T> row)
{
    logger.log("Matrix::printRow");
    ofstream fout(this->sourceFileName, ios::app);
    this->writeRow(row, fout);
    fout.close();
}
};
