<h1>Data Systems Report - Project Phase 1
</h1>

February 2025

<h3>Project Team - 11</h3>

Bhavani Chalasani (2022101014)

Miryala Sathvika (2023121007)

Vasana Srinivasan (2022101023) 


<h2>Implementation:</h2>

<h3>Reasons for using tile-based blocking:</h3>

We have used tile-based blocking for the following reasons:

*   **Efficient Fetching** - Row-wise access loads full rows, even if only part of the data is needed and column-wise access does the same with full columns, leading to slow row reconstruction. Blocking fetches small tiles instead of entire rows or columns, so instead of loading a full row/column from disk, only relevant blocks are fetched. This results in faster memory access, fewer disk reads, and lower latency.
    

*   **Efficient Storing** - Tiles group related data together, allowing efficient writing in contiguous blocks instead of scattered memory locations, which saves time and memory bandwidth, making writes more efficient.
    

*   **Efficient Loading** - When loading data into cache, row-wise and column-wise access cause frequent cache misses. Blocking loads only small tiles into cache, keeping them in memory long enough to be fully processed, resulting in faster computation and fewer cache misses.
    

<h3>Explanation of the Matrix Struct:</h3>

The Matrix class manages matrix data storage, retrieval, and transformations with support for block-based operations. It provides functionalities for loading, storing, transposing, rotating, and checking properties like antisymmetry.  

<h3>1. SOURCE</h3>
    

**Syntax:** **SOURCE**

**Logic:** This query takes input from a .ra file, and sequentially runs all the commands listed in the file. The filename provided in the query will not have the “.ra” extension. The extension is appended in the code, after which the file is opened and parsed line by line until the end, after which the file is closed. Each line is considered a separate query.

**Error Handling:** The code throws a semantic error if the file does not exist. Every query in the file is parsed both semantically and syntactically before it is executed. 

<h3>2. LOAD</h3>
    

**Syntax:** **LOAD MATRIX**

**Logic and Error Handling:** This function reads a matrix from the given file and loads it into the system by storing the attributes of the matrix in a Matrix object and splitting the contents of the matrix into pages. The code first checks whether the given matrix already exists in the matrix catalogue, in which case it gives a semantic error. It then checks whether the file exists (it appends the “.csv” extension in the code), and returns a semantic error if the file doesn't exist. In case of success, a new Matrix object is created and load() is called. This opens the file and reads in the first line. If the file is empty, it returns false (load failed). It then closes the file. The first line read is split into columns using the commas and the number of columns of the matrix is counted and stored in the Matrix object. After this, the blockifyMatrix() function is called to divide the matrix into blocks and write the blocks to the disk. 

<h3>Page Design: blockifyMatrix()</h3>

Tile-based blocking is done, and the record organization is unspanned. The matrix is divided into fixed-size square tiles (blocks) for efficient storage and processing. First, rows are read in chunks of blockSize. Each chunk is then split into smaller column-wise blocks of blockSize × blockSize. A new tile is initialised with zeros and filled with the corresponding values from the chunk. These blocks are stored separately, allowing efficient operations on portions of the matrix instead of the entire matrix at once.

<h3>3. PRINT</h3>
    

**Syntax: PRINT MATRIX**  

**Logic**: The function retrieves and prints the matrix in blocks based on its column count.

If the matrix has fewer than 17 columns (because the number of elements we can store in a single block is 16x16), it fetches and prints a single block.

If the matrix has 17 or more columns, it retrieves two blocks at a time and merges their values while printing.

For larger matrices, it moves to the next row block using the Cursor and continues printing.

**Block access pattern**: Row-wise, fetching blocks sequentially and handling cases where columns span multiple blocks.

**Error Handling:** The code throws a semantic error if the matrix does not exist in the matrix catalogue. 

<h3>4. EXPORT</h3>
    

**Syntax: EXPORT MATRIX**

**Logic:** The function writes the matrix to a CSV file by retrieving it block by block and reconstructing full rows.

If the matrix is not already permanent, it deletes any existing file.

It iterates row by row, processing each block row separately.

For each block row, it fetches the required blocks column-wise, reconstructs the row, and writes it to the file.

**Block Access Pattern:** Row-wise access, fetching blocks sequentially from left to right in each block row. It uses the Cursor to move between blocks while maintaining row continuity.

**Error Handling:** The code throws a semantic error if the matrix does not exist in the matrix catalogue. 

<h3>5. ROTATE</h3>
    

**Logic:** The function rotates the matrix 90 degrees clockwise by:

*   Transposing blocks: Swaps elements across the diagonal (i, j with j, i).
    
*   Flipping blocks horizontally: Swaps blocks in each row from left to right.
    

**Block Access Pattern:**

*   Row-major access for transposition (fetching blocks diagonally).
    
*   Column-wise swaps for horizontal flipping (processing each row separately).
    
*   Uses Cursor to fetch blocks and bufferManager.writePage() to update them.
    

**Error Handling:** The code throws a semantic error if the matrix does not exist in the matrix catalogue. 

<h3>6. CROSS TRANSPOSE</h3>
    

**Syntax: CROSSTRANSPOSE**

**Logic:** The function cross-transposes two matrices by swapping their corresponding blocks. It determines the smaller matrix and the larger matrix, and iterates over the common block count and swaps corresponding blocks between the two matrices.It updates block metadata (rowsPerBlockCount, colsPerBlockCount, rowcount, columncount, and blockcount). If one matrix has more blocks, it copies the extra blocks to the other matrix and deletes them from the original.

**Block access pattern:** It uses block-wise swapping through Cursor and BufferManager. Blocks are retrieved row-wise, and then written to their new positions. If one matrix has more blocks, the remaining blocks are copied sequentially from one to the other and deleted.

**Error Handling:** The code throws a semantic error if either of the two matrices do not exist in the matrix catalogue. 


<h3>7. CHECK ANTI-SYMMETRY</h3>
    

**Syntax: CHECKANTISYM** 

**Logic:** The function checks if a matrix is antisymmetric by verifying that for every element A\[i\]\[j\], A\[i\]\[j\]=−B\[j\]\[i\] holds for a second matrix B. It iterates over blocks and retrieves them using cursors. It checks if A\[i\]\[j\]=−B\[j\]\[i\] for each pair of elements.

**Block Access Pattern:** Uses Cursor to retrieve blocks from both matrices, and iterates row-wise over the upper triangle of the matrix to check antisymmetry. Block-wise access is used to compare corresponding transposed elements. Blocks are fetched in (i, j) and (j, i) pairs.

**Error Handling:** The code throws a semantic error if either of the two matrices do not exist in the matrix catalogue, and ensures both matrices have the same dimension, and verifies that corresponding blocks have the same row and column counts.

<h2>Assumptions</h2>

1.  All inputted matrices will be square. 
    
2.  If a matrix that has 0 rows and 0 columns is loaded, it will not be stored in the matrix catalogue. (similar to a table).
    
3.  One additional block stored in memory is always of size 16x16.
    

<h2>Individual Contribution</h2>

**SOURCE:** Bhavani, Vasana

**LOAD:** Sathvika

**PRINT:** Sathvika, Bhavani, Vasana

**EXPORT:** Sathvika

**ROTATE:** Sathvika, Bhavani, Vasana

**CROSS TRANSPOSE:** Sathvika, Bhavani, Vasana

**ANTI SYMMETRY:** Bhavani, Vasana