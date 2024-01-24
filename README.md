# TransactionsCount MapReduce

## Overview
The **TransactionsCount** MapReduce program is part of an assignment for the University of Macedonia, designed to explore the intricacies of distributed data processing using the Hadoop MapReduce framework. The primary objective is to count transactions based on combinations of words within large datasets.
This program is designed to process large datasets and count transactions based on combinations of words. It utilizes the Hadoop MapReduce framework to efficiently distribute the processing across a cluster of machines.

## Methodology

The implementation offers two distinct approaches:

1. **Single Job Method:**
   - The first method employs a single MapReduce job to process the data, demonstrating a straightforward yet comprehensive approach.

2. **Chained Jobs Method:**
   - The second method involves a two-step MapReduce process, showcasing the benefits of modularization and efficiency through job chaining.

## Comparison

Both methods aim to achieve the same goal — counting transactions based on word combinations. The comparison between the single job and chained jobs methods explores factors such as performance, resource utilization, and ease of implementation.


## Functionality

1. **Mapper:**
   - Tokenizes input lines and generates combinations of words.
   - Emits each combination of words as a key with a count of 1.

2. **Combiner:**
   - Optional step to combine intermediate results locally on each mapper.
   - In this implementation, the combiner acts as a local reducer by summing counts.

3. **Reducer:**
   - Aggregates counts for each combination of words.
   - Filters results based on a specified transaction threshold.

4. **Configuration:**
   - Allows users to set a transaction threshold through a command-line argument.

## How to Use

1. **Input:**
   - Provide a text input file where each line represents a transaction.

2. **Output:**
   - The output consists of combinations of words and their corresponding transaction counts.

3. **Command for Single Job::**
   ```bash
   hadoop jar TransactionsCount.jar inputPath outputPath transactionThreshold numReduceTasks

* **inputPath:** Path to the input dataset.
* **outputPath:** Path to store the MapReduce job output.
* **transactionThreshold:** Minimum transaction count to include in the output.
* **numReduceTasks:** Number of reduce tasks for parallel processing.

   **Command for Chained Jobs:** 
    ```bash
    hadoop jar TransactionsCount.jar inputPath outputPath intermediatePath finalOutputPath threshold numReduceTasks
    
 * **inputPath:** Path to the input dataset.
 * **outputPath:** Path to store the output in the single job method or the first MapReduce job in the chained jobs method.
 * **intermediatePath:** Path to store intermediate results in the chained jobs method.
 * **finalOutputPath:** Path to store the final MapReduce job output in the chained jobs method.
 * **threshold:** Minimum transaction count to include in the final output.
 * **numReduceTasks:** Number of reduce tasks for parallel processing.

## Notes

* The use of a combiner is optional and can be adjusted based on the specific use case.
* Considerations for job chaining include efficiency gains and modularity.
* Evaluate the trade-offs between the single job and chained jobs methods based on performance metrics and specific use cases.
* Ensure the Hadoop environment is properly configured before running the program.

