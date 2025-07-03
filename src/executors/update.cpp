#include "global.h"

/**
 * @brief
 * SYNTAX: UPDATE table_name WHERE condition SET col_name = value
 */

bool syntacticParseUPDATE()
{
    logger.log("syntacticParseUPDATE");

    if (tokenizedQuery.size() != 10 || tokenizedQuery[0] != "UPDATE" ||
        tokenizedQuery[2] != "WHERE" || tokenizedQuery[6] != "SET" || tokenizedQuery[8] != "=")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType                = UPDATE;
    parsedQuery.updateRelation           = tokenizedQuery[1];
    parsedQuery.updateconditionColumn    = tokenizedQuery[3];
    parsedQuery.updateconditionValue     = stoi(tokenizedQuery[5]);

    string op = tokenizedQuery[4];
    if      (op == "<")  parsedQuery.updateBinaryOperator = LESS_THAN;
    else if (op == "<=") parsedQuery.updateBinaryOperator = LEQ;
    else if (op == ">")  parsedQuery.updateBinaryOperator = GREATER_THAN;
    else if (op == ">=") parsedQuery.updateBinaryOperator = GEQ;
    else if (op == "==") parsedQuery.updateBinaryOperator = EQUAL;
    else if (op == "!=") parsedQuery.updateBinaryOperator = NOT_EQUAL;
    else {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.updateColumn = tokenizedQuery[7];
    parsedQuery.updateValue  = stoi(tokenizedQuery[9]);
    return true;
}

bool semanticParseUPDATE()
{
    logger.log("semanticParseUPDATE");

    if (!tableCatalogue.isTable(parsedQuery.updateRelation)) {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (!tableCatalogue.isColumnFromTable(parsedQuery.updateconditionColumn,
                                         parsedQuery.updateRelation) ||
        !tableCatalogue.isColumnFromTable(parsedQuery.updateColumn,
                                         parsedQuery.updateRelation))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void applyUpdatesFromL2(Table *sourceTable,
                        Cursor &sourceCursor,
                        Table *indexL1, Table *indexL2,
                        int key, 
                        int updateColIdx,
                        int newValue,
                        int flag) 
{
    parsedQuery.searchBinaryOperator = EQUAL;
    int l1Idx = binarySearcher(indexL1, key);
    if (l1Idx < 0) return;         

    Cursor c1 = indexL1->getCursor();
    c1.nextPage(l1Idx);
    auto l1Block = c1.getNextBlock();

    int off = -1;
    for (int r = 0; r < (int)l1Block.size(); ++r) {
        if (l1Block[r][0] == key) { off = r; break; }
    }
    if (off < 0) return;
    auto &l1row = l1Block[off];
    for (int slot = 1; slot < (int)l1row.size(); ++slot) {
        int l2PageNo = l1row[slot];
        if (l2PageNo < 0) break;
        Cursor c2 = indexL2->getCursor();
        c2.nextPage(l2PageNo);
        auto l2Block = c2.getNextBlock();
        for (auto &pr : l2Block) {
            int srcPage = pr[0], srcSlot = pr[1];
            sourceCursor.nextPage(srcPage);
            auto pageRows = sourceCursor.getNextBlock();
            pageRows[srcSlot][updateColIdx] = newValue;
            bufferManager.writePage(
                parsedQuery.updateRelation,
                srcPage,
                pageRows, pageRows.size(),
                /*flags=*/7, 0,
                parsedQuery.updateRelation,
                pageRows
            );
        }
    }
}

void executeUPDATE()
{
    logger.log("executeUPDATE");
    Table *sourceTable = tableCatalogue.getTable(parsedQuery.updateRelation);
    int updateColIdx = sourceTable->getColumnIndex(parsedQuery.updateColumn);
    string L1name = parsedQuery.updateRelation + parsedQuery.updateconditionColumn + "L1";
    if (!tableCatalogue.isTable(L1name)) {
        parsedQuery.indexColumnName = parsedQuery.updateconditionColumn;
        parsedQuery.indexRelationName = parsedQuery.updateRelation;
        executeINDEX();
    }
    string L2name = parsedQuery.updateRelation + parsedQuery.updateconditionColumn + "L2";
    Table *indexL1 = tableCatalogue.getTable(L1name);
    Table *indexL2 = tableCatalogue.getTable(L2name);
    int condVal = parsedQuery.updateconditionValue;
    int op = parsedQuery.updateBinaryOperator;
    Cursor c1 = indexL1->getCursor();
    for (int pg = 0; pg < indexL1->blockCount; ++pg) {
        c1.nextPage(pg);
        auto l1Block = c1.getNextBlock();
        for (auto &row : l1Block) {
            int key = row[0];
            bool match = false;
            switch (op) {
                case LESS_THAN:       match = (key <  condVal); break;
                case LEQ:             match = (key <= condVal); break;
                case GREATER_THAN:    match = (key >  condVal); break;
                case GEQ:             match = (key >= condVal); break;
                case EQUAL:           match = (key == condVal); break;
                case NOT_EQUAL:       match = (key != condVal); break;
            }
            if (match) {
                Cursor sourceCursor = sourceTable->getCursor();
                applyUpdatesFromL2(
                    sourceTable, sourceCursor,
                    indexL1, indexL2,
                    key, updateColIdx,
                    parsedQuery.updateValue,
                    /*flag=*/op
                );
            }
        }
    }
        string tbl = parsedQuery.updateRelation;
        string updCol = parsedQuery.updateColumn;

        L1name = tbl + updCol + "L1";
        L2name = tbl + updCol + "L2";

        if (tableCatalogue.isTable(L1name)) {
            tableCatalogue.deleteTable(L1name);
            logger.log("Dropped stale index table: " + L1name);
        }
        if (tableCatalogue.isTable(L2name)) {
            tableCatalogue.deleteTable(L2name);
            logger.log("Dropped stale index table: " + L2name);
        }
    return;
}
