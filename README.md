# SimpleRA: A Disk-Based Relational Algebra Query Processor

[![Language](https://img.shields.io/badge/Language-C%2B%2B-blue.svg)](https://isocpp.org/)
[![Build](https://img.shields.io/badge/Build-Makefile-brightgreen.svg)](https://www.gnu.org/software/make/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

**SimpleRA** is a custom-built, disk-based relational database management system (RDBMS) developed from the ground up in C++.

The system's architecture includes abuffer manager, a block-based storage engine, and a multi-level secondary index to ensure high performance for data retrieval and modification. In addition to standard relational tables, SimpleRA has been extended to natively support matrix data structures and their associated algebraic operations.

## Core Features

*   **Relational Algebra Query Engine**: Implements a wide range of relational algebra operators for complex data analysis, including:
    *   **Query Operations**: `SELECT`, `PROJECT`, `JOIN`, `CROSS`, `DISTINCT`.
    *   **Sorting & Aggregation**: `SORT`, `ORDER BY`, `GROUP BY` with aggregate functions (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`).
    *   **Data Management**: `LOAD`, `PRINT`, `EXPORT`, `CLEAR`, `RENAME`.

*   **Matrix Operations**: A unique extension that treats matrices as first-class citizens, with specialized commands for linear algebra tasks:
    *   `LOAD MATRIX` / `PRINT MATRIX` / `EXPORT MATRIX`
    *   `ROTATE`: In-place 90-degree clockwise rotation of a matrix.
    *   `CROSSTRANSPOSE`: Swaps the transposed blocks of two matrices.
    *   `CHECKANTISYM`: Verifies if a matrix is anti-symmetric against another.

*   **Disk-Based Storage & Memory Management**:
    *   **Buffer Manager**: Employs a page-based buffer pool with a FIFO replacement policy to efficiently manage I/O between disk and main memory.
    *   **Block-Based Storage**: All tables and matrices are partitioned into fixed-size blocks (pages) on disk, allowing the system to handle datasets larger than available RAM.
    *   **Cursor Model**: Provides a robust, iterator-like cursor for traversing records within tables and blocks.

*   **Advanced Indexing for Performance**:
    *   **Two-Level Secondary Index**: Implements a secondary index to dramatically accelerate data lookups. This index maps attribute values to record pointers, avoiding costly full-table scans.
    *   **Optimized DML Operations**: `SEARCH`, `INSERT`, `UPDATE`, and `DELETE` commands are optimized to leverage the index for high-speed execution.
    *   **Lazy Deletion & Dirty Flags**: Features an efficient deletion mechanism that marks rows for removal and uses a "dirty index" flag to ensure indexes are only rebuilt when necessary, minimizing overhead on subsequent operations.

*   **Large-Scale Data Operations**:
    *   **External Merge Sort**: The `SORT` and `GROUP BY` commands use a 2-phase k-way external merge sort to handle sorting relations that do not fit in memory.
    *   **Partition Hash Join**: Implements an efficient hash join for equijoins by partitioning relations into buckets that can be processed in memory.

## Architecture Overview

The system processes queries through a well-defined pipeline:

1.  **Command Input**: The server accepts a query from the user.
2.  **Syntactic Parser**: The query string is tokenized and validated against the defined grammar to ensure correct syntax.
3.  **Semantic Parser**: The parsed query is checked for logical correctness (e.g., tables and columns exist, data types match). It also handles index status checks and triggers rebuilds if a "dirty" index is used.
4.  **Executor**: A dedicated execution function for the specific command is called.
5.  **Core Components**: The executor interacts with the system's core components to perform its task:
    *   **Table/Matrix Catalogue**: Manages metadata for all loaded relations.
    *   **Buffer Manager**: Fetches data pages from disk into memory.
    *   **Cursor**: Iterates over the records within the fetched pages.

## Getting Started

### Prerequisites

*   A C++ compiler (`g++`)
*   `make` build automation tool
*   Python 3 (for generating test data)

### Compilation

1.  Navigate to the source directory:
    ```bash
    cd src
    ```

2.  Clean previous builds and compile the project:
    ```bash
    make clean
    make
    ```
    This will create a `server` executable in the `src` directory.

### Generating Test Data

A Python script is provided to generate a sample integer dataset.

1.  From the root directory, run the `tester.py` script:
    ```bash
    python3 tester.py
    ```
2.  This will create a file named `integer_dataset.csv` inside the `data/` directory.

### Running the Server

1.  From the `src` directory, run the server executable:
    ```bash
    ./server
    ```
2.  The server will start, and you can begin typing queries at the `>` prompt.

### Example Usage

```sql
-- Load a table from a CSV file
> LOAD integer_dataset

-- Print the first 20 rows of the table
> PRINT integer_dataset

-- Create a secondary index on 'col_1'
> INDEX ON col_1 FROM integer_dataset USING BTREE

-- Create a new table with values from col_1 greater than 500
> high_vals <- SEARCH FROM integer_dataset WHERE col_1 > 500

-- Print the new table
> PRINT high_vals

-- Run a script of commands from a file
> SOURCE my_queries

-- Exit the application
> QUIT
```

## Supported Commands

A summary of supported commands. For full grammar, see `Grammar.md`.

#### Table & Data Management

| Command                                                    | Description                                            |
| ---------------------------------------------------------- | ------------------------------------------------------ |
| `LOAD <relation_name>`                                     | Loads a `.csv` file from the `data/` directory.        |
| `PRINT <relation_name>`                                    | Prints the first 20 rows of a table.                   |
| `EXPORT <relation_name>`                                   | Saves a table to a permanent `.csv` file.              |
| `CLEAR <relation_name>`                                    | Removes a table from the system.                       |
| `LIST TABLES`                                              | Lists all loaded tables.                               |
| `RENAME <old_col> TO <new_col> FROM <relation_name>`         | Renames a column in a table.                           |
| `INDEX ON <col> FROM <relation_name> USING BTREE`          | Creates a two-level secondary index on a column.       |
| `INSERT INTO <relation_name> (col1 = val1, ...)`           | Inserts a new row into a table and updates indices.    |
| `UPDATE <relation_name> WHERE <cond> SET <col> = <val>`      | Updates rows matching a condition and updates indices. |
| `DELETE FROM <relation_name> WHERE <cond>`                 | Lazily deletes rows matching a condition.              |
| `SOURCE <filename>`                                        | Executes a batch of commands from a `.ra` file.        |
| `QUIT`                                                     | Exits the server.                                      |

#### Relational Algebra Operations

| Command                                                               | Description                                                               |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `<new> <- SELECT <cond> FROM <relation>`                              | Selects rows based on a condition.                                        |
| `<new> <- SEARCH FROM <relation> WHERE <cond>`                        | An index-optimized version of SELECT.                                     |
| `<new> <- PROJECT <cols> FROM <relation>`                             | Creates a new table with a subset of columns.                             |
| `<new> <- CROSS <relation1> <relation2>`                              | Computes the Cartesian product of two tables.                             |
| `<new> <- JOIN <rel1>, <rel2> ON <col1> == <col2>`                     | Joins two tables based on an equi-join condition using a hash join.       |
| `<new> <- DISTINCT <relation>`                                        | *Syntax supported, implementation pending.*                               |
| `<new> <- SORT <relation> BY <col> IN <ASC/DESC>`                     | Creates a new sorted table using external merge sort.                     |
| `<new> <- ORDER BY <col> <ASC/DESC> ON <relation>`                    | Creates a new sorted table (alias for SORT).                              |
| `<new> <- GROUP BY <g_col> FROM <rel> HAVING <agg_cond> RETURN <agg>` | Groups rows and computes aggregate functions.                             |

#### Matrix Operations

| Command                          | Description                                         |
| -------------------------------- | --------------------------------------------------- |
| `LOAD MATRIX <matrix_name>`      | Loads a matrix from a `.csv` file.                  |
| `PRINT MATRIX <matrix_name>`     | Prints a matrix to the console.                     |
| `EXPORT MATRIX <matrix_name>`    | Saves a matrix to a permanent `.csv` file.          |
| `ROTATE <matrix_name>`           | Rotates a matrix 90 degrees clockwise in-place.     |
| `CROSSTRANSPOSE <mat1> <mat2>`   | Transposes and swaps the blocks of two matrices.    |
| `CHECKANTISYM <mat1> <mat2>`     | Checks if `mat1` is anti-symmetric with respect to `mat2`. |

## Team & Contributions

This project was developed by a team of three:
*   **Miryala Sathvika (2023121007)**
*   **Bhavani Chalasani (2022101014)** 
*   **Vasana Srinivasan (2022101023)**

Key areas of contribution included:
*   **Data Aggregation and Loading**: Implementation of the `GROUP BY` operator and the `LOAD` functionality for both tables and matrices.
*   **Advanced Indexing and DML**: Design and implementation of the two-level secondary index and its integration with `INSERT`, `UPDATE`, and `DELETE` commands.
*   **Matrix Subsystem**: Extension of the core storage engine to support matrix data structures and their corresponding operations.
