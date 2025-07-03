#include "global.h"

/**
 * @brief
 * SYNTAX: R <- DISTINCT relation_name
 */
bool syntacticParseCROSSTRANSPOSE()
{
    logger.log("syntacticParseCROSSTRANSPOSE");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CROSSTRANSPOSE;
    parsedQuery.checkAntiSymRelation1 = tokenizedQuery[1];
    parsedQuery.checkAntiSymRelation2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCROSSTRANSPOSE()
{
    logger.log("semanticParseCROSSTRANSPOSE");
    // The resultant table shouldn't exist and the table argument should
    if (!matrixCatalogue.isMatrix(parsedQuery.checkAntiSymRelation1))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }

    if (!matrixCatalogue.isMatrix(parsedQuery.checkAntiSymRelation2))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }
    return true;
}
void printMatrixAttributes(Matrix* matrix) {
    cout<<"*************"<<endl;
    cout << "Matrix Name: " << matrix->matrixName << endl;
    cout << "Source File Name: " << matrix->sourceFileName << endl;
    cout << "Rows: " << matrix->rows << endl;
    cout << "Cols: " << matrix->cols << endl;
    cout << "Row Count: " << matrix->rowCount << endl;
    cout << "Column Count: " << matrix->columnCount << endl;
    cout << "Block Count: " << matrix->blockCount << endl;
    cout << "Matrix Column Count: " << matrix->matrixColumnCount << endl;
    cout << "Max Rows Per Block: " << matrix->maxRowsPerBlock << endl;
    cout << "Max Cols Per Block: " << matrix->maxColsPerBlock << endl;
    cout << "Indexed: " << (matrix->indexed ? "true" : "false") << endl;
    cout << "Indexed Column: " << matrix->indexedColumn << endl;

    cout << "Columns Per Block: ";
    for (const auto& col : matrix->columns)
        cout << col << " ";
    cout << endl;

    cout << "Rows Per Block: ";
    for (const auto& val : matrix->rowsPerBlock)
        cout << val << " ";
    cout << endl;

    cout << "Rows Per Block Count: ";
    for (const auto& val : matrix->rowsPerBlockCount)
        cout << val << " ";
    cout << endl;

    cout << "Cols Per Block Count: ";
    for (const auto& val : matrix->colsPerBlockCount)
        cout << val << " ";
    cout << endl;
}

void executeCROSSTRANSPOSE()
{
    logger.log("executeCROSSTRANSPOSE");
    Matrix *matrix1 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymRelation1);
    Matrix *matrix2 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymRelation2);
    // printMatrixAttributes(matrix1);
    // printMatrixAttributes(matrix2);
    matrix1->TransposeMatrix();
    matrix2->TransposeMatrix();
    // cout << "BEGIN" << endl;
    // printMatrixAttributes(matrix1);
    // printMatrixAttributes(matrix2);
    // cout << "Matrix Column Count: " << matrix1->matrixColumnCount << endl;
    // cout << "Matrix Column Count: " << matrix2->matrixColumnCount << endl;
    matrix1->crossTranspose(matrix2->matrixName);
    // cout << "Matrix Column Count: " << matrix1->matrixColumnCount << endl;
    // cout << "Matrix Column Count: " << matrix2->matrixColumnCount << endl;
    // printMatrixAttributes(matrix1);
    // printMatrixAttributes(matrix2);
    // std::cout << "Before Swap:\n";
    // std::cout << "Matrix1: " << matrix1->matrixName << " (" << matrix1->rows << "x" << matrix1->cols << ")\n";
    // std::cout << "Matrix2: " << matrix2->matrixName << " (" << matrix2->rows << "x" << matrix2->cols << ")\n";

    // // Print some vector sizes to check if swapping caused issues

    // std::cout << "Matrix1 rowsPerBlock size: " << matrix1->rowsPerBlockCount.size() << "\n";
    // std::cout << "Matrix2 rowsPerBlock size: " << matrix2->rowsPerBlockCount.size() << "\n";

    // std::cout << "Matrix1 rowsPerBlockCount: ";
    // for (int value : matrix1->rowsPerBlockCount)
    // {
    //     std::cout << value << " ";
    // }
//     std::cout << std::endl;
//     std::cout << "Matrix2 rowsPerBlockCount: ";
//     for (int value : matrix2->rowsPerBlockCount)
//     {
//         std::cout << value << " ";
//     }
//     std::cout << std::endl;


//     std::cout << "Matrix1 colsPerBlockCount: ";
// for (int value : matrix1->colsPerBlockCount)
// {
//     std::cout << value << " ";
// }
// std::cout << std::endl;

// std::cout << "Matrix2 colsPerBlockCount: ";
// for (int value : matrix2->colsPerBlockCount)
// {
//     std::cout << value << " ";
// }
// std::cout << std::endl;





//     swap(*matrix1, *matrix2);
//     std::cout << "After Swap:\n";
//     std::cout << "Matrix1: " << matrix1->matrixName << " (" << matrix1->rows << "x" << matrix1->cols << ")\n";
//     std::cout << "Matrix2: " << matrix2->matrixName << " (" << matrix2->rows << "x" << matrix2->cols << ")\n";

//     // Print some vector sizes to check if swapping caused issues

//     std::cout << "Matrix1 rowsPerBlock size: " << matrix1->rowsPerBlockCount.size() << "\n";
//     std::cout << "Matrix2 rowsPerBlock size: " << matrix2->rowsPerBlockCount.size() << "\n";

//     std::cout << "Matrix1 rowsPerBlockCount: ";
//     for (int value : matrix1->rowsPerBlockCount)
//     {
//         std::cout << value << " ";
//     }
//     std::cout << std::endl;
//     std::cout << "Matrix2 rowsPerBlockCount: ";
//     for (int value : matrix2->rowsPerBlockCount)
//     {
//         std::cout << value << " ";
//     }
//     std::cout << std::endl;


//     std::cout << "Matrix1 colsPerBlockCount: ";
// for (int value : matrix1->colsPerBlockCount)
// {
//     std::cout << value << " ";
// }
// std::cout << std::endl;

// std::cout << "Matrix2 colsPerBlockCount: ";
// for (int value : matrix2->colsPerBlockCount)
// {
//     std::cout << value << " ";
// }
// std::cout << std::endl;


    // matrix1->TransposeMatrix();
    // matrix2->TransposeMatrix();

    // string temp1=matrix1->sourceFileName;
    // matrix1->sourceFileName=matrix2->sourceFileName;
    // matrix2->sourceFileName=temp1;

    // string temp2=matrix1->matrixName;
    // matrix1->matrixName=matrix2->matrixName;
    // matrix2->matrixName=temp2;

    // vector<string>temp3=matrix1->columns;
    // matrix1->columns.clear();
    // matrix1->columns.assign(matrix2->columns.begin(), matrix2->columns.end());
    // matrix2->columns.clear();
    // matrix2->columns.assign(temp3.begin(), temp3.end());

    // long long int temp4=matrix1->columnCount;
    // matrix1->columnCount=matrix2->columnCount;
    // matrix2->columnCount=temp4;

    // long long int temp5=matrix1->rowCount;
    // matrix1->rowCount=matrix2->rowCount;
    // matrix2->rowCount=temp5;

    // int temp6=matrix1->rows;
    // matrix1->rows=matrix2->rows;
    // matrix2->rows=temp6;

    // int temp7=matrix1->cols;
    // matrix1->cols=matrix2->cols;
    // matrix2->cols=temp7;

    // uint temp8=matrix1->blockCount;
    // matrix1->blockCount=matrix2->blockCount;
    // matrix2->blockCount=temp8;

    // vector<int>temp9=matrix1->rowsPerBlock;
    // matrix1->rowsPerBlock.clear();
    // matrix1->rowsPerBlock.assign(matrix2->rowsPerBlock.begin(), matrix2->rowsPerBlock.end());
    // matrix2->rowsPerBlock.clear();
    // matrix2->rowsPerBlock.assign(temp9.begin(), temp9.end());

    // int temp10=matrix1->matrixColumnCount;
    // matrix1->matrixColumnCount=matrix2->matrixColumnCount;
    // matrix2->matrixColumnCount=temp10;

    // int temp11=matrix1->maxRowsPerBlock;
    // matrix1->maxRowsPerBlock=matrix2->maxRowsPerBlock;
    // matrix2->maxRowsPerBlock=temp11;

    // int temp12=matrix1->maxColsPerBlock;
    // matrix1->maxColsPerBlock=matrix2->maxColsPerBlock;
    // matrix2->maxColsPerBlock=temp12;

    // vector<int>temp13=matrix1->rowsPerBlockCount;
    // matrix1->rowsPerBlockCount.clear();
    // matrix1->rowsPerBlockCount.assign(matrix2->rowsPerBlockCount.begin(), matrix2->rowsPerBlockCount.end());
    // matrix2->rowsPerBlockCount.clear();
    // matrix2->rowsPerBlockCount.assign(temp13.begin(), temp13.end());

    // vector<int>temp14=matrix1->colsPerBlockCount;
    // matrix1->colsPerBlockCount.clear();
    // matrix1->colsPerBlockCount.assign(matrix2->colsPerBlockCount.begin(), matrix2->colsPerBlockCount.end());
    // matrix2->colsPerBlockCount.clear();
    // matrix2->colsPerBlockCount.assign(temp14.begin(),temp14.end());

    // bool temp15=matrix1->indexed;
    // matrix1->indexed=matrix2->indexed;
    // matrix2->indexed=temp15;

    // string temp16=matrix1->indexedColumn;
    // matrix1->indexedColumn=matrix2->indexedColumn;
    // matrix2->indexedColumn=temp16;

    // cout<<"DONE"<<endl;
    return;
}