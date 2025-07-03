# Data Systems 

## Phase 2 Report  
### By Bhavani Chalasani (2022101014), Miryala Sathvika(2023121007), and Vasana Srinivasan (2022101023)

## Explanation of Implementation  

### SORT: Implementation of External Sorting  
This implementation of external sorting follows a **2-phase k-way merge sort** approach. First, in `executeSORT`, the relation is divided into multiple sorted sublists (each fitting within the available memory). The blocks of the relation are read into main memory, sorted and made into sorted (using `sortMatrix`) and merged sublists (`mergeSortedBuffers`). At the end of the sort phase, we will have up to **k-1 sublists**, where each sublist is sorted and merged among its own blocks.  

Next, the merge phase begins, where a **min-heap (priority queue)** is used to merge sorted sublists. During merging, the smallest element (from the current block of each sublist - we consider one block of each sublist at a time) is extracted, and its successor is pushed back into the heap. The sorted results are written into new pages in batches, maintaining the correct block structure. The process repeats for **2 phases at most** until only one fully sorted run remains. Finally, old intermediate files are deleted, and the sorted relation is reconstructed.  

---

### ORDER BY: Application of External Sorting  
For this, a new table is created, which is a copy of the table to order by, and the table is sorted as per the column given using the external sort described above.  

---

### GROUP BY: Application of External Sorting  
For this implementation, we first extract the necessary columns to create the new result table. Next, we sort the input table using an external sort function on the group-by column. This sorting ensures that rows with the same grouping key are adjacent, which simplifies the grouping process.

We maintain three data structures to store the relevant column values:

- Group-by Column: Contains the keys used to group the rows.

- Aggregate Column for HAVING Clause: Contains values on which an aggregate function is applied to filter groups via a HAVING clause.

- Aggregate Column for Return: Contains values on which an aggregate function is computed to produce the final result.

Helper functions are used to convert binary operations to the required symbols, and the external sort function organizes the required columns accordingly.

As the processing proceeds, values for each group are collected in vectors. When the group changes, aggregate functions are computed for the accumulated values. A buffer is used to temporarily hold the results in memory (in 9 blocks) before writing them to disk, thereby reducing the frequency of disk writes and improving I/O performance.

---

### Partition Hash Join  
For this, the two files are first individually **hashed into 9 (k-1) buckets**, and the buckets are written to the disk. A **hashmap** is used to track the pages allocated for each bucket. This is done for both the relations that we want to perform the join on. After this, the corresponding buckets from each relation are read into memory (using the hashmap to find the pages), and the possible tuples are joined with the **equi-join condition** and written to the new table, which contains columns from both files. The new table is sorted after all the joins are completed.  

---

## Assumptions  
- The **k-way merge sort** implemented is **2-phase**. Hence, according to the two constraints that **B/(M-1) ≤ M**, and the number of main memory buffers **(M) is 10**, only tables with a maximum of **B = 90 blocks** can be sorted, ordered by, joined, or grouped by.  
- The above assumption means that the table after joining must be **≤ 90 blocks**.  

---

## Contributions  
- **Implementation of External Sorting:** Bhavani & Vasana  
- **Order By:** Bhavani & Vasana  
- **Group By:** Sathvika  
- **Partition Hash Join:** Bhavani & Vasana  
