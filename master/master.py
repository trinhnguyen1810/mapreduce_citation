import os
import logging
import json
import threading
from flask import Flask, request, jsonify
from collections import defaultdict
from queue import Queue
import requests
import time
import heapq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = Flask(__name__)

class MasterNode:
    def __init__(self):
        # initialize address
        self.host = '0.0.0.0'
        self.port = int(os.getenv('PORT', 5001))

        #job config 
        self.num_mappers = int(os.getenv('NUM_MAPPERS', 2))
        self.num_reducers = int(os.getenv('NUM_REDUCERS', 2))
        self.num_partitions = 4

        # file paths that system need to know
        self.input_file = os.getenv('INPUT_FILE', 'data/cit-HepTh.txt')
        self.output_file = os.getenv('OUTPUT_FILE', 'output/final_citation_counts.txt')
        #holding each partition data
        self.partitions = {} 

        # system to track task
        # using queue to get task processed in order
        self.map_queue = Queue()
        self.map_in_progress = set()
        self.map_completed = set()
        
        self.reduce_queue = Queue()
        self.reduce_in_progress = set()
        self.reduce_completed = set()
        
        # tracking address of worker
        self.workers = {}  # {worker_id: (host, port)}
        
        # single lock for task state updates
        self.state_lock = threading.Lock()
        
        # control flags
        self.job_started = False
        self.all_tasks_completed = False
        
        # initialize flask app
        self.app = app
        self.setup_routes()

    def setup_routes(self):
        @app.route('/register', methods=['POST'])
        #work config endpoint so master can keep track of the worker
        def register_worker():
            data = request.json
            worker_id = data['worker_id']

            #adding worker to register
            with self.state_lock:
                self.workers[worker_id] = (data['host'], data['port'])
            logging.info(f"Registered worker {worker_id} at {data['host']}:{data['port']}")
            return jsonify({"status": "registered"})

        # task request system that assign next task in queue for worker 
        @app.route('/request_task', methods=['POST'])
        def request_task():
            worker_id = request.json['worker_id']
            task = self.assign_task(worker_id)
            if task:
                return jsonify(task)
            return jsonify({"status": "no_task"})

        # endpoint workers can notify task completion
        @app.route('/task_complete', methods=['POST'])
        def task_complete():
            data = request.json
            self.handle_task_completion(data)
            return jsonify({"status": "processed"})

    #paritioning the data
    def partition_data(self):
        try:
            logging.info(f"Reading input file: {self.input_file}")
            # counting the input files and splitting into the number of 
            # paritions that we have configured
            total_lines = sum(1 for line in open(self.input_file) 
                            if line.strip() and not line.startswith('#'))
            
            lines_per_split = total_lines // self.num_partitions
            current_partition = []
            partition_id = 0
            
            with open(self.input_file, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        current_partition.append(line.strip())
                        
                        if len(current_partition) >= lines_per_split and partition_id < self.num_partitions - 1:
                            self.partitions[partition_id] = current_partition
                            # add each task (the paritions) to the queue so map would begin processing
                            self.map_queue.put(partition_id)
                            logging.info(f"Created partition {partition_id} with {len(current_partition)} lines")
                            current_partition = []
                            partition_id += 1
                
                # add remaining lines to last partition
                if current_partition:
                    self.partitions[partition_id] = current_partition
                    self.map_queue.put(partition_id)
                    logging.info(f"Created final partition {partition_id} with {len(current_partition)} lines")
            
            logging.info(f"Data partitioned into {len(self.partitions)} splits")
            self.job_started = True
            
        except Exception as e:
            logging.error(f"Error partitioning data: {e}")
            raise

    # add next assigned task in queue to wroker
    def assign_task(self, worker_id):
        with self.state_lock:
            # if the worker request is mapper and queue is not empty
            # assign the mapper worker the next task in queue
            if worker_id.startswith('mapper'):
                if not self.map_queue.empty():
                    partition_id = self.map_queue.get()
                    self.map_in_progress.add(partition_id)
                    logging.info(f"Assigning map task for partition {partition_id} to {worker_id}")
                    return {
                        'type': 'map',
                        'partition_id': partition_id,
                        'data': self.partitions[partition_id]
                    }
                
             # if the worker request is mapper and queue is not empty
            # assign the reducer worker the next task in queue
            elif worker_id.startswith('reducer'):
                if not self.reduce_queue.empty():
                    partition_id = self.reduce_queue.get()
                    self.reduce_in_progress.add(partition_id)
                    logging.info(f"Assigning reduce task for partition {partition_id} to {worker_id}")
                    return {
                        'type': 'reduce',
                        'partition_id': partition_id
                    }
        return None

    #handling transitioning from map to reduce
    def handle_task_completion(self, data):
        worker_id = data['worker_id']
        task_type = data['task_type']
        partition_id = data['partition_id']
        
        with self.state_lock:
            if task_type == 'map':
                self.map_in_progress.remove(partition_id)
                # add that the new map task is done
                self.map_completed.add(partition_id)
                logging.info(f"Map task for partition {partition_id} completed")
                
                # if map phase is complete
                if len(self.map_completed) == self.num_partitions:
                    #start reduce phase
                    self.start_reduce_phase()
                    
            elif task_type == 'reduce':
                self.reduce_in_progress.remove(partition_id)
                self.reduce_completed.add(partition_id)
                logging.info(f"Reduce task for partition {partition_id} completed")
                
                # check if reduce phase is complete
                if len(self.reduce_completed) == self.num_reducers:
                    self.all_tasks_completed = True
                    #then aggregrae results
                    self.aggregate_results()

    #put reduce tasks to queue and process it
    def start_reduce_phase(self):
        logging.info("Starting reduce phase")
        for i in range(self.num_reducers):
            self.reduce_queue.put(i)

    def aggregate_results(self):
        logging.info("Aggregating final results")
        try:
            # combine all counts
            final_counts = defaultdict(int)
            for partition_id in range(self.num_reducers):
                #read the reduce file, aggregrate and write final result
                reduce_file = f"output/reduce_{partition_id}.txt"
                with open(reduce_file, 'r') as f:
                    for line in f:
                        paper_id, count = line.strip().split(',')
                        final_counts[paper_id] += int(count)
            
            # write results with formatting
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with open(self.output_file, 'w') as f:
                # write header
                f.write("=" * 50 + "\n")
                f.write("Top 30 Most Cited Papers\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"{'Rank':<6}{'Paper ID':<15}{'Citations':>10}\n")
                f.write("-" * 31 + "\n")
                
                # Write sorted results with ranking
                for rank, (paper_id, total_count) in enumerate(
                    sorted(final_counts.items(), key=lambda x: (-x[1], x[0]))[:30], 1):
                    f.write(f"{rank:<6}{paper_id:<15}{total_count:>10,}\n")
                    logging.info(f"Rank {rank}: Paper {paper_id} with {total_count:,} citations")
                
                # Write footer
                f.write("\n" + "-" * 31 + "\n")
                f.write(f"Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            logging.info("Final results written successfully")
                
        except Exception as e:
            logging.error(f"Error aggregating results: {e}")


    def start(self):
        os.makedirs('output', exist_ok=True)
        
        # start Flask in a separate thread
        server = threading.Thread(target=self.app.run, 
                                kwargs={'host': self.host, 'port': self.port},
                                )
        server.start()
        
        # start the job by partioning the data
        logging.info("Starting data partitioning")
        self.partition_data()
        
        # monitor job progress
        while not self.all_tasks_completed:
            time.sleep(1)
            with self.state_lock:
                logging.info(f"Progress - Map: {len(self.map_completed)}/{self.num_partitions}, "
                           f"Reduce: {len(self.reduce_completed)}/{self.num_reducers}")
  
        server.join()
if __name__ == "__main__":
    master = MasterNode()
    #start master node
    try:
        master.start()
    except KeyboardInterrupt:
        logging.info("Master node shutting down")