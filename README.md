# MapReduce System

This is a distributed MapReduce system implemented using Python and Flask. It consists of a master node and multiple worker nodes (mappers and reducers) that work together to process large datasets.

## Setup

To set up, make sure you have Docker installed in your machine. After that, download the folder or clone it. Additionally, to test, make sure that the input file (`cit-HepTh.txt`) is placed in the `data` directory. 
1. Navigate to the project directory.
2. Open a terminal and run the following command to start the MapReduce system:

   ```
   docker compose up --build
   ```

   or run in detached mode (so there aren't many logs overwhelming)


   ```
   docker compose up -d --build
   ```

   This command will build the necessary Docker images and start the containers for the master node and worker nodes. It will automatically start the mapreduce process

4. The master node will be accessible at `localhost:5001`, and the worker nodes will be running on the following ports:
   - Mapper 1: 5002
   - Mapper 2: 5003 
   - Reducer 1: 5004
   - Reducer 2: 5005


5. To view the final result, run the following command:

   ```
   cat output/final_citation_counts.txt
   ```

   This command will display the contents of the final output file.

6. To stop the MapReduce system and remove the containers, run the following command:

   ```
   docker compose down
   ```

## Checking Results

A `check.py` script is provided to process the citation data and display the top 30 most cited papers without running the entire MapReduce system. To use it:


Just simply run `python check.py` in the project directory and compare results

All intermediate files after mapping and reducing process can also be viewed in `output` directory.

## Shortcomings

While the current MapReduce system is functional, it has some shortcomings that need to be addressed:

- Single point of failure: The system relies on a single master node at a specified port, which can become a bottleneck and a single point of failure as the system scales with no restarting mechanism or dynammic assignment to another node.
- In-memory data partitioning: The input data is loaded into memory and partitioned by the master node, which may not be efficient for large datasets.
- Lack of fault tolerance: The system does not have robust mechanisms to handle incomplete tasks in case of worker node failures.
- Centralized task assignment: The task assignment is managed using a centralized queue, which may not scale well as the number of worker nodes increases.

For more detailed information about the shortcomings and potential improvements, please refer to the comprehensive report included in the project.