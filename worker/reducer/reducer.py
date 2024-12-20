import os
import logging
import requests
import time
import heapq
import threading
from flask import Flask, request, jsonify
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

app = Flask(__name__)

class ReduceWorker:
    def __init__(self):
        # initialize address
        self.host = '0.0.0.0'
        self.port = int(os.getenv('WORKER_PORT', 5003))
        #get reducer work id
        self.worker_id = os.getenv('WORKER_ID', f'reducer_{self.port}')
        self.master_host = os.getenv('MASTER_HOST', 'master')
        #get master address
        self.master_port = int(os.getenv('MASTER_PORT', 5001))
        #flag to see if current node is processing
        self.is_processing = False
        self.app = app
        self.setup_routes()
        
    def setup_routes(self):
        # define route to handle reduce tasks
        @self.app.route('/task', methods=['POST'])
        def handle_task():
            task = request.json
            if task['type'] != 'reduce':
                # reject non-reduce tasks
                return jsonify({"error": "not a reduce task"}), 400
            
            if not self.is_processing:
                # start processing task in a separate thread
                threading.Thread(target=self._process_reduce_task, args=(task,)).start()
                return jsonify({"status": "accepted"}), 200
            else:
                # reject if already processing a task
                return jsonify({"error": "currently processing"}), 409

    def _process_reduce_task(self, task):
        try:
            self.is_processing = True
            reducer_id = task['partition_id']
            logging.info(f"starting reduce task for partition {reducer_id}")
            # add delay here for testing
            #logging.info("Starting 15 second delay for testing...")
            #time.sleep(15)
            #logging.info("Delay complete, continuing processing...")
        
            
            # find all map output files for the given reducer
            map_files = [f for f in os.listdir('output') 
                        if f.startswith('map_') and f'for_reducer_{reducer_id}' in f]
            logging.info(f"found {len(map_files)} map files to process")
            
            # initialize min heap and file handles
            min_heap = []
            file_handles = []
            
            # load the first entry from each file into the heap
            for map_file in map_files:
                try:
                    file_handle = open(f"output/{map_file}", 'r')
                    file_handles.append(file_handle)
                    line = file_handle.readline()
                    if line:
                        paper_id, count = line.strip().split(',')
                        heapq.heappush(min_heap, (paper_id, int(count), len(file_handles) - 1))
                except Exception as e:
                    logging.error(f"error processing map file {map_file}: {e}")
            
            # perform merge-sort using the heap
            current_paper_id = None
            current_count = 0
            output_file = f"output/reduce_{reducer_id}.txt"
            
            with open(output_file, 'w') as out_file:
                while min_heap:
                    paper_id, count, file_index = heapq.heappop(min_heap)
                    
                    # write the previous paper_id if a new one is encountered
                    if current_paper_id and current_paper_id != paper_id:
                        out_file.write(f"{current_paper_id},{current_count}\n")
                        current_count = 0
                    
                    # update the current paper and its count
                    current_paper_id = paper_id
                    current_count += count
                    
                    # read the next line from the same file
                    next_line = file_handles[file_index].readline()
                    if next_line:
                        next_paper_id, next_count = next_line.strip().split(',')
                        heapq.heappush(min_heap, (next_paper_id, int(next_count), file_index))
                
                # write the last paper_id to the output file
                if current_paper_id:
                    out_file.write(f"{current_paper_id},{current_count}\n")
            
            # close all file handles
            for handle in file_handles:
                handle.close()
            
            # notify master of task completion
            self._notify_completion(reducer_id)
            logging.info(f"completed reduce task for partition {reducer_id}")
            
        except Exception as e:
            logging.error(f"error in reduce task: {e}")
        finally:
            self.is_processing = False

    def _notify_completion(self, partition_id):
        # notify master that the reduce task is complete
        try:
            response = requests.post(
                f"http://{self.master_host}:{self.master_port}/task_complete",
                json={
                    'worker_id': self.worker_id,
                    'task_type': 'reduce',
                    'partition_id': partition_id
                }
            )
            response.raise_for_status()
        except Exception as e:
            logging.error(f"error notifying completion: {e}")

    def request_task(self):
        # request a new task from the master
        if self.is_processing:
            return False
            
        try:
            response = requests.post(
                f"http://{self.master_host}:{self.master_port}/request_task",
                json={'worker_id': self.worker_id}
            )

            
            if response.status_code == 200:
                task = response.json()
                if task.get('type') == 'reduce':
                    self._process_reduce_task(task)
                    return True
                elif task.get('status') == 'no_task':
                    time.sleep(5)
            return False
        except Exception as e:
            logging.error(f"error requesting task: {e}")
            return False

    def register_with_master(self):
        # register the worker with the master
        try:
            response = requests.post(
                f"http://{self.master_host}:{self.master_port}/register",
                json={
                    'worker_id': self.worker_id,
                    'host': self.host,
                    'port': self.port
                }
            )
            response.raise_for_status()
            logging.info("registered with master")
            return True
        except Exception as e:
            logging.error(f"error registering with master: {e}")
            return False

    def start(self):
        # start the worker server and handle task processing loop
        server = threading.Thread(target=self.app.run, 
                                kwargs={'host': self.host, 'port': self.port},
                                daemon=True)
        server.start()
        
        # keep trying to register with the master until successful
        while not self.register_with_master():
            logging.info("retrying registration in 5 seconds...")
            time.sleep(5)
        
        # continuously request tasks and process them
        while True:
            try:
                if not self.is_processing:
                    self.request_task()
                time.sleep(1)
            except KeyboardInterrupt:
                break

# entry point for the reduce worker
if __name__ == "__main__":
    worker = ReduceWorker()
    try:
        worker.start()
    except KeyboardInterrupt:
        logging.info("reduce worker shutting down")