# Data Systems 
## Phase 3 Report 
By Bhavani Chalasani (2022101014), Miryala Sathvika, and Vasana Srinivasan (2022101023)

# Explanation of Implementation 
`Index Structure`: A secondary index structure (2-level) has been implemented. For our implementation, we have one level of indirection - there is an index file with <K(i), P(i)> entries where K(i) is the field value and P(i) is a list of block pointers to blocks of record pointers, where each record pointer points to a record with the field value as K(i) (this is hereby referred to as the L1 index). The block pointer points to a block of record pointers which has entries of the type <B(i),R(i)> where B(i) is the block address and R(i) is the index of the row within the block where the row corresponding to K(i) is located(hereby referred to as the L2 index).  Both indices will always be in sorted order - the index file will be in sorted order of the field values and the block of record pointers will be sorted in order of the block address B(i). In the L1 index, in order to fill all the columns, we consider the number of columns to be the maximum number of L2 blocks for a given field value, and we pad any rows with a lower number of columns than this with -1. 

To summarise, 
`L1 table`: Maps distinct source table values to L2 blocks
`L2 table`: Maps from L1 entries to specific row locations in the source table
Note : Only 70 percent of pages are filled to further make the inserts easier.
## Search
- The query is syntactically and semantically parsed. After that, if the required index doesn't exist, it calls executeINDEX() to create one. The binarySearcher() function then locates the appropriate block in the L1 index (using binary search) that might contain the target value based on the specified comparison operator (EQUAL, LESS_THAN, GREATER_THAN, LEQ, GEQ). The search() function processes rows from the L1 index, retrieves corresponding L2 index entries, and finally accesses the actual data rows from the source table. Based on the comparison operator it follows different strategies:
- For GREATER_THAN/GEQ: Finds the first row in the identified block that satisfies the condition, then processes all subsequent rows in that block and all following blocks.
- For LESS_THAN/LEQ: Processes all blocks before the identified block fully, then processes the identified block up to the row where the condition no longer holds.
- For EQUAL: Finds the exact row in the L1 block and processes only the matching entries.
- For NOT_EQUAL: Processes all blocks but skips rows that match the search value.
- For each qualifying row identified through the indexes, it adds it to the result table. Our approach here is efficient because every qualifying block from the result table is retrieved only once - since the L2 index is sorted by block value, once a result table block is retrieved, all the qualifying rows within the block are processed, and the block need not be retrieved again. Further, finding the block in the L1 index with the value to search for is done via binary search and not a linear scan. 
 

## Delete
- The query is syntactically and semantically parsed. After that, if the required index doesn't exist, it calls executeINDEX() to create one. The binarySearcher() function then locates the appropriate block in the L1 index (using binary search) that might contain the target value based on the specified comparison operator (EQUAL, LESS_THAN, GREATER_THAN, LEQ, GEQ). The delete1() function processes rows from the L1 index, retrieves corresponding L2 index entries, and finally accesses the actual data rows from the source table. Based on the comparison operator it follows different strategies:
- For GREATER_THAN/GEQ: Finds the first row in the identified block that satisfies the condition, then processes all subsequent rows in that block and all following blocks.
- For LESS_THAN/LEQ: Processes all blocks before the identified block fully, then processes the identified block up to the row where the condition no longer holds.
- For EQUAL: Finds the exact row in the L1 block and processes only the matching entries.
- For NOT_EQUAL: Processes all blocks but skips rows that match the search value.
- For each qualifying row identified through the indexes, it adds a tuple <x,y> (where x is the block number and y is the row number within the block) to a DeletionMap, which is an ordered map, specific to each table. Whenever delete1() is called, we increment a deleteCount variable, which is specific to each table, and keeps track of the number of rows to be deleted.
- When any command other than delete is called after this and the deleteCount of the table is greater than zero, the deleteRowsFromTable_Blocks() function is called, which shifts the rows within the pages of the table, resulting in table pages that no longer contain the rows to be deleted (as stored in the DeletionMap). This will leave empty space within the page. The DeletionMap is then cleared. Then, the deleteRowsFromTable_Threshold() function is called, which updates the entire table by pushing all the blocks together to form the updated table, leaving no empty spaces, post deletion. We have added this here, rather than deleting the rows when the delete command is called, so that successive deletes can be efficient - the deletion map will keep getting updated on successive deletes, but the actual table rearrangement will only be done once, when another command which requires the updated table, is called. On rearranging the table, we set a dirtyIndex variable for each index on that table (which keeps track of whether the index has been updated and needs to be rebuilt), to true. This variable is initially set to false on creation of the index table. If the command is SORT, we also delete all the indexes on that table, because the position of the rows will change on SORT, so the indexes becomes invalid. When we call a phase 3 command (SORT, DELETE, UPDATE), we check if the dirtyIndex variable is set to true, If so, we rebuild the indexes - we delete it, rebuild both the L1 and L2 indexes, and set the dirtyIndex variable of the index to be used to false. This is done in the buildIndex() function. This is more efficient than rebuilding each index corresponding to a table, as we are only updating the indexes of the column that is relevant for the corresponding phase 3 command. And this is only done if the table has been modified (if the dirtyIndex is true) so the index is not unnecessarily created each time a phase 3 operation is called. For each function call, we are accessing each block of the table once. We are performing this check for deleteCount in semanticParser.cpp, which we have edited to perform semantic parsing, perform this check and then perform semantic parsing again(because in case a table has been completely deleted after the updation happens for example, semantic parsing needs to happen again).

- Insert: For the insertion we always add the new rows into the end of the sourcetable. Now, to update the index, we iterate on all the columns for the table. If the column has no index, we continue to the next column. We first check whether there exists the value before in the table, if yes, we will check the l2 blocks for the key, as the l2 blocks are only 70 percent filled, we may find the space and add the new inserted row’s block pointer and row pointer. If there are no l2 blocks left, or no space to insert the l2 blocks , then we delete the index. If the value is not there in the l1, we will check where should the value fit, if it is in the last page and the last page is full we add a new page.However, if it is in any of the middle pages and they are full we delete the index. After allocating the l1 block, we then allocate the l2 blocks and add the new row and block pointer.

- Update:For the update we use the search in the previously described and find all the blocks that need to be modified. After finding the blocks, we change the value of the column as given in the query and using the buffermanager, we write this page into the disk. We delete the index on the updated column as changing one value may impact the index alot and take as many as blocks needed for building the new index.



## Assumptions:
The indices are stored as tables of the form tableName+IndexColumnName+”L1” for the L1 index and tableName+IndexColumnName+”L2” for the L2 index. No tables must be added with the same names to the system. It is assumed that the possibility of a user doing so is extremely low, and hence we have decided this naming convention.


## Contributions:
- Implementation of Index Structure: Bhavani & Vasana
- Search: Bhavani & Vasana 
- Delete: Bhavani & Vasana 
- Insert: Sathvika
- Update: Sathvika

