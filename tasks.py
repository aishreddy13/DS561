from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np
import networkx as nx
import concurrent.futures
from scipy.sparse import dok_matrix, csr_matrix

# Initialize the Google Cloud Storage client
storage_client = storage.Client()

# Replace 'your-bucket-name' with the name of your bucket
bucket_name = 'u62138442_hw2_b2'

# Get a reference to the bucket
bucket = storage_client.bucket(bucket_name)

# Initialize dictionaries to store incoming and outgoing link counts
incoming_counts = {}
outgoing_counts = {}

# List all blobs (files) in the bucket
blobs = list(bucket.list_blobs())

# Iterate through the blobs and analyze HTML files
def process_html_file(blob):
    if blob.name.endswith('.html'):
        # Download HTML file content as bytes
        html_content_bytes = blob.download_as_bytes()
        html_content_str = html_content_bytes.decode('utf-8')

        soup = BeautifulSoup(html_content_str, 'html.parser')

        incoming_count = 0
        outgoing_count = 0

        for anchor_tag in soup.find_all('a'):
            href = anchor_tag.get('href')
            if href:
                if href.endswith('.html'):
                    outgoing_count += 1
                    linked_file_name = href.split('/')[-1]
                    incoming_counts[linked_file_name] = incoming_counts.get(linked_file_name, 0) + 1
                else:
                    incoming_count += 1

        outgoing_counts[blob.name] = outgoing_count

# Define the number of concurrent workers
num_workers = 4  # Adjust as needed
batch_size = 200  # Adjust as needed

# Use concurrent.futures to process files in parallel in batches
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    # Split blobs into batches
    for i in range(0, len(blobs), batch_size):
        batch = blobs[i:i + batch_size]
        # Process the batch of files
        executor.map(process_html_file, batch)


# Print incoming and outgoing link counts for all HTML files
for file_name, incoming_count in incoming_counts.items():
    outgoing_count = outgoing_counts[file_name]
    # print(f"File: {file_name}")
    # print(f"Incoming Links: {incoming_count}")
    # print(f"Outgoing Links: {outgoing_count}")
    # print("=" * 40)
    # Calculate statistics for incoming and outgoing links
    incoming_links_values = list(incoming_counts.values())
    outgoing_links_values = list(outgoing_counts.values())

# Calculate and print the statistics
avg_incoming = np.mean(incoming_links_values)
median_incoming = np.median(incoming_links_values)
max_incoming = max(incoming_links_values)
min_incoming = min(incoming_links_values)
quintiles_incoming = np.percentile(incoming_links_values, [20, 40, 60, 80])

avg_outgoing = np.mean(outgoing_links_values)
median_outgoing = np.median(outgoing_links_values)
max_outgoing = max(outgoing_links_values)
min_outgoing = min(outgoing_links_values)
quintiles_outgoing = np.percentile(outgoing_links_values, [20, 40, 60, 80])

print("Statistics for Incoming Links:")
print(f"Average: {avg_incoming}")
print(f"Median: {median_incoming}")
print(f"Max: {max_incoming}")
print(f"Min: {min_incoming}")
print(f"Quintiles: {quintiles_incoming}")

print("\nStatistics for Outgoing Links:")
print(f"Average: {avg_outgoing}")
print(f"Median: {median_outgoing}")
print(f"Max: {max_outgoing}")
print(f"Min: {min_outgoing}")
print(f"Quintiles: {quintiles_outgoing}")

# num_pages = len(outgoing_counts)
# pagerank = np.full(num_pages, 1.0 / num_pages)
# damping_factor = 0.85
# convergence_threshold = 0.005
# iteration = 0

# while True:
#     new_pagerank = np.full(num_pages, (1.0 - damping_factor) / num_pages)

#     for i, page in enumerate(outgoing_counts):
#         for linking_page, incoming_count in incoming_counts.items():
#             if page != linking_page and page in outgoing_counts:
#                 num_links = outgoing_counts[linking_page]
#                 if num_links != 0:
#                     new_pagerank[i] += damping_factor * pagerank[i] * incoming_count / num_links

#     total_change = np.sum(np.abs(new_pagerank - pagerank))
#     pagerank = new_pagerank
#     iteration += 1

#     if total_change < convergence_threshold:
#         break
# sorted_pages = np.argsort(pagerank)[::-1][:5]

# for i in sorted_pages:
#     print(f"Page: {list(outgoing_counts.keys())[i]}, Rank: {pagerank[i]:.6f}")

# Create a list of all unique page names (nodes)
all_pages = list(set(incoming_counts.keys()).union(outgoing_counts.keys()))
num_pages = len(all_pages)

# Create a sparse link matrix (dok_matrix) for the graph
link_matrix = dok_matrix((num_pages, num_pages), dtype=np.float32)

for linking_page, incoming_count in incoming_counts.items():
    if linking_page not in outgoing_counts:
        continue
    total_outgoing_links = outgoing_counts[linking_page]
    if total_outgoing_links != 0:
        for page in outgoing_counts:
            if page != linking_page:
                row_idx = all_pages.index(linking_page)
                col_idx = all_pages.index(page)
                link_matrix[row_idx, col_idx] = incoming_count / total_outgoing_links
    else:
        for page in outgoing_counts:
            if page != linking_page:
                row_idx = all_pages.index(linking_page)
                col_idx = all_pages.index(page)
                link_matrix[row_idx, col_idx] = 1.0 / num_pages

# Convert the link matrix to a compressed sparse row (CSR) matrix
link_matrix_csr = csr_matrix(link_matrix)

# Calculate PageRank using the power iteration method
damping_factor = 0.85
num_iterations = 100  # You can adjust the number of iterations

# Initialize the PageRank vector
pagerank = np.ones(num_pages, dtype=np.float32) / num_pages

for _ in range(num_iterations):
    # Perform matrix-vector multiplication
    pagerank = damping_factor * link_matrix_csr.dot(pagerank) + (1 - damping_factor) / num_pages

sorted_pages = [(page, rank) for page, rank in zip(all_pages, pagerank)]
sorted_pages.sort(key=lambda x: x[1], reverse=True)
top_pages = sorted_pages[:5]

# Output the top 5 pages by PageRank score
for page, rank in top_pages:
    print(f"Page: {page}, Rank: {rank:.6f}")
