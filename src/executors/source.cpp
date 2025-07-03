#include "global.h"
#include <cstdio>
/**
 * @brief
 * SYNTAX: SOURCE filename
 */
bool syntacticParseSOURCE()
{
    logger.log("syntacticParseSOURCE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SOURCE;
    parsedQuery.sourceFileName = tokenizedQuery[1];
    return true;
}

bool semanticParseSOURCE()
{
    logger.log("semanticParseSOURCE");
    if (!isQueryFile(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        return false;
    }
    return true;
}

// this function checks whether the .ra query file exists in the data folder
// .ra is appended, not given in the user query
bool isQueryFileExists(string queryfilename)
{
    string fileName = "../data/" + queryfilename;
    struct stat buffer;
    return (stat(fileName.c_str(), &buffer) == 0);
}

void executeSOURCE()
{
    logger.log("executeSOURCE");
    string temp = parsedQuery.sourceFileName;
    parsedQuery.sourceFileName += ".ra";
    if (!isQueryFileExists(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: Query file doesn't exist" << endl;
        return;
    }

    string oldfile = "../data/" + parsedQuery.sourceFileName;
    string newfile = "../data/" + temp + ".txt";
    rename(oldfile.c_str(), newfile.c_str());

    fstream queryfile;
    queryfile.open(newfile, ios::in);
    regex delim("[^\\s,]+");
    if (queryfile.is_open())
    {
        string line;
        while (getline(queryfile, line))
        {
            // cout << line << endl;
            logger.log(line);
            tokenizedQuery.clear();
            auto words_begin = std::sregex_iterator(line.begin(), line.end(), delim);
            auto words_end = std::sregex_iterator();
            for (std::sregex_iterator i = words_begin; i != words_end; ++i)
                tokenizedQuery.emplace_back((*i).str());
            if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
            {
                break;
            }

            if (tokenizedQuery.empty())
            {
                continue;
            }

            if (tokenizedQuery.size() == 1)
            {
                cout << "SYNTAX ERROR" << endl;
                continue;
            }

            logger.log("doCommand");
            if (syntacticParse() && semanticParse())
                executeCommand();
        }
        queryfile.close();
    }
    rename(newfile.c_str(), oldfile.c_str());
    return;
}