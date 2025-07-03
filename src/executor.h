#include"semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeLOADMATRIX();
void executePRINTMATRIX();
void executeEXPORTMATRIX();
void executeROTATE();
void executeCROSSTRANSPOSE();
void executeCHECKANTISYM();
void executeORDERBY();
void executeGROUPBY();
void executeSEARCH();
void executeDELETE();
void executeINSERT();
void executeUPDATE();
int binarySearcher(Table* table, int searchVal);
void deleteRowsFromTable_Threshold(Table *table);
void deleteRowsFromTable_Blocks(Table *table);
void buildIndex(Table *table, string indexName);
bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);