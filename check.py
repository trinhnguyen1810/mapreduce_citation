import logging
from collections import defaultdict


def process_citations_simple(file_path):
    """
    processes a citation file and directly computes the results without partitioning.
    Sorts and prints the most cited papers.
    """
    # dictionary to store results: to_node -> list of from_nodes
    citation_map = defaultdict(list)

    try:
        # open and process the file
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue  # skip empty lines and comments
                
                try:
                    from_node, to_node = line.split('\t')
                    citation_map[to_node].append(from_node)
                except ValueError as e:
                    logging.error(f"Malformed line: {line} - {e}")
        
        # sort by citation count using predefined library dunction
        sorted_citations = sorted(citation_map.items(), key=lambda item: len(item[1]), reverse=True)

        # display the top 10 most cited papers
        print("Top 10 most cited papers:")
        for idx, (to_node, from_nodes) in enumerate(sorted_citations[:30], 1):
            print(f"{idx}. {to_node}: {len(from_nodes)} citations")
    except Exception as e:
        logging.error(f"Error processing file: {e}")

# running main file
if __name__ == "__main__":
    input_file = 'data/cit-HepTh.txt' 
    process_citations_simple(input_file)
