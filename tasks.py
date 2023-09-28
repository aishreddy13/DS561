from google.cloud import storage
from bs4 import BeautifulSoup
import numpy as np

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
for blob in blobs:
    if blob.name.endswith('.html'):
        # Download HTML file content as bytes
        html_content_bytes = blob.download_as_bytes()

        # Convert bytes to a string assuming UTF-8 encoding
        html_content_str = html_content_bytes.decode('utf-8')

        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content_str, 'html.parser')

        # Find all anchor (a) tags in the HTML
        anchor_tags = soup.find_all('a')

        # Initialize counts for this HTML file
        incoming_count = 0
        outgoing_count = 0

        for anchor_tag in anchor_tags:
            href = anchor_tag.get('href')
            if href:
                # Check if the href attribute contains a link to another HTML file
                if href.endswith('.html'):
                    outgoing_count += 1
                    # Check if the linked HTML file exists in the same bucket
                    linked_file_name = href.split('/')[-1]
                    if linked_file_name in incoming_counts:
                        incoming_counts[linked_file_name] += 1
                    else:
                        incoming_counts[linked_file_name] = 1
                else:
                    incoming_count += 1

        # Store counts in dictionaries
        incoming_counts[blob.name] = incoming_count
        outgoing_counts[blob.name] = outgoing_count

incoming_links_values = {}
outgoing_links_values = {}

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

# Initialize PageRank values for all pages
num_pages = len(outgoing_counts)  # Assuming outgoing_counts contains all pages
pagerank = {page: 1.0 / num_pages for page in outgoing_counts}

# Define the convergence threshold (0.5% change)
convergence_threshold = 0.005

# Perform iterative PageRank calculation
# Perform iterative PageRank calculation
while True:
    new_pagerank = {}
    total_change = 0

    for page in outgoing_counts:
        new_pagerank[page] = 0.15  # Initialize with the constant 0.15

        # Calculate the contribution from incoming links
        for linking_page, incoming_count in incoming_counts.items():
            if page != linking_page and page in outgoing_counts:
                num_links = outgoing_counts[linking_page]
                # Check if num_links is non-zero before division
                if num_links != 0:
                    new_pagerank[page] += 0.85 * pagerank[linking_page] / num_links

        total_change += abs(new_pagerank[page] - pagerank[page])

    pagerank = new_pagerank

    # Check for convergence
    if total_change < convergence_threshold:
        break


# Sort pages by PageRank and get the top 5
top_pages = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:5]

# Output the top 5 pages by PageRank score
for page, rank in top_pages:
    print(f"{page}: {rank}")

