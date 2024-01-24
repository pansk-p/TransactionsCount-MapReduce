# TransactionsCount MapReduce

## Overview

The **TransactionsCount** MapReduce program is designed to process large datasets and count transactions based on combinations of words. It utilizes the Hadoop MapReduce framework to efficiently distribute the processing across a cluster of machines.

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

3. **Command:**
   ```bash
   hadoop jar TransactionsCount.jar inputPath outputPath transactionThreshold numReduceTasks

  * **inputPath:** Path to the input dataset.
  * **outputPath:** Path to store the MapReduce job output.
  * **transactionThreshold:** Minimum transaction count to include in the output.
  * **numReduceTasks:** Number of reduce tasks for parallel processing.

## Notes

  * The use of a combiner is optional and can be adjusted based on the specific use case.
  * Ensure the Hadoop environment is properly configured before running the program.

