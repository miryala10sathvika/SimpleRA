#include "global.h"

void MatrixCatalogue::insertMatrix(Matrix* matrix)
{
    logger.log("MatrixCatalogue::~insertMatrix"); 
    this->matrices[matrix->matrixName] = matrix;
}

void MatrixCatalogue::deleteMatrix(string matrixName)
{
    logger.log("MatrixCatalogue::deletematrix"); 
    this->matrices[matrixName]->unload();
    delete this->matrices[matrixName];
    this->matrices.erase(matrixName);
}
Matrix* MatrixCatalogue::getMatrix(string matrixName)
{
    logger.log("MatrixCatalogue::getmatrix"); 
    Matrix *matrix = this->matrices[matrixName];
    
    return matrix;
}
bool MatrixCatalogue::isMatrix(string matrixName)
{
    logger.log("MatrixCatalogue::ismatrix"); 
    if (this->matrices.count(matrixName))
        return true;
    return false;
}


void MatrixCatalogue::print()
{
    logger.log("MatrixCatalogue::print"); 
    cout << "\nRELATIONS" << endl;

    int rowCount = 0;
    for (auto rel : this->matrices)
    {
        cout << rel.first << endl;
        rowCount++;
    }
    printRowCount(rowCount);
}

MatrixCatalogue::~MatrixCatalogue(){
    logger.log("MatrixCatalogue::~MatrixCatalogue"); 
    for(auto matrix: this->matrices){
        matrix.second->unload();
        delete matrix.second;
    }
}