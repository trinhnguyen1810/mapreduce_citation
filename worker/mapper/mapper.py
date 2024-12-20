import os
import logging
import requests
import time
import threading
import hashlib
from flask import Flask, request, jsonify
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

#initialize Flask app
app = Flask(__name__)

class MapWorker:
    def __init__(self):
        # initialize address
        self.host = '0.0.0.0'
        self.port = int(os.getenv('WORKER_PORT', 5002))
        #get mapper work id
        self.worker_id = os.getenv('WORKER_ID', f'mapper_{self.port}')
        self.master_host = os.getenv('MASTER_HOST', 'master')
        # address of master node
        self.master_port = int(os.getenv('MASTER_PORT', 5001))
        self.num_reducers = int(os.getenv('NUM_REDUCERS', 2))

        #flag to check whether current mapper is processinh
        self.is_processing = False
        #setting up flask app
        self.app = app
        self.setup_routes()

    def get_reducer_for_paper(self, paper_id):
        #hashes the paper ID to determine which reducer will process it
        hash_val = int(hashlib.md5(paper_id.encode()).hexdigest(), 16)
        return hash_val % self.num_reducers

    def setup_routes(self):
        # sets up flask routes for the worker to handle incoming task
        @self.app.route('/task', methods=['POST'])
        def handle_task():
            task = request.json
            if task['type'] != 'map':
                return jsonify({"error": "Not a map task"}), 400
            
            #if it is available process the task as requested
            if not self.is_processing:
                threading.Thread(target=self._process_map_task, args=(task,)).start()
                return jsonify({"status": "accepted"}), 200
            #if it is not return Dalse
            else:
                return jsonify({"error": "Currently processing"}), 409

    #main function process map task assigned each wroker
    def _process_map_task(self, task):
        try:
            self.is_processing = True
            map_id = task['partition_id']
            logging.info(f"Processing partition {map_id}")
            # add delay here for testing
            #logging.info("Starting 15 second delay for testing...")
            #time.sleep(15)
            #logging.info("Delay complete, continuing processing...")
            
            # initialize output buffers for each reducer
            reducer_buffers = defaultdict(lambda: defaultdict(int))
            
            # process input data
            for line in task['data']:
                try:
                    from_node, to_node = line.split('\t')
                    # determine which reducer should handle this paper
                    reducer_id = self.get_reducer_for_paper(to_node)
                    reducer_buffers[reducer_id][to_node] += 1
                except Exception as e:
                    logging.error(f"Error processing line '{line}': {e}")

            # write output files for each reducer (save results to output files)
            os.makedirs('output', exist_ok=True)
            for reducer_id, counts in reducer_buffers.items():
                if counts:  # write file if there's data
                    output_file = f"output/map_{map_id}_for_reducer_{reducer_id}.txt"
                    with open(output_file, 'w') as f:
                        for paper_id, count in counts.items():
                            f.write(f"{paper_id},{count}\n")
                    logging.info(f"Wrote {len(counts)} counts to {output_file}")

            # notify the master node of task completion
            self._notify_completion(map_id)
            logging.info(f"Completed partition {map_id}")
            
        except Exception as e:
            logging.error(f"Error in map task: {e}")
        finally:
            self.is_processing = False

    # notify the master node of task completion of a particular partition
    def _notify_completion(self, partition_id):
        try:
            response = requests.post(
                f"http://{self.master_host}:{self.master_port}/task_complete",
                json={
                    'worker_id': self.worker_id,
                    'task_type': 'map',
                    'partition_id': partition_id
                }
            )
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error notifying completion: {e}")

    #request new task from masternode
    def request_task(self):
        #make sure if its available, if it is not dont request
        if self.is_processing:
            return False
            
        try:
            response = requests.post(
                f"http://{self.master_host}:{self.master_port}/request_task",
                json={'worker_id': self.worker_id}
            )
            if response.status_code == 200:
                task = response.json()
                if task.get('type') == 'map':
                    self._process_map_task(task)
                    return True
                elif task.get('status') == 'no_task':
                    time.sleep(5)
            return False
        except Exception as e:
            logging.error(f"Error requesting task: {e}")
            return False

    #register worker with master (initial start up phase)
    def register_with_master(self):
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
            logging.info("Registered with master")
            return True
        except Exception as e:
            logging.error(f"Error registering with master: {e}")
            return False

    #starts the worker by running the Flask server and handling tasks.
    def start(self):
        server = threading.Thread(target=self.app.run, 
                                kwargs={'host': self.host, 'port': self.port},
                                daemon=True)
        server.start()
        
        while not self.register_with_master():
            logging.info("Retrying registration in 5 seconds...")
            time.sleep(5)
        
        while True:
            try:
                #if the worker is available, request task in the queue
                if not self.is_processing:
                    self.request_task()
                time.sleep(1)
            except KeyboardInterrupt:
                break

if __name__ == "__main__":
    worker = MapWorker()
    try:
        worker.start()
    except KeyboardInterrupt:
        logging.info("Map worker shutting down")