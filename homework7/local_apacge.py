import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio
from datetime import datetime
import logging

# Setup logging
logging.getLogger().setLevel(logging.INFO)

# Function to parse files and extract links
def parse_file(element):
    filename, file_content = element
    links = re.findall(r'<a\s+[^>]*\bhref=["\']([^"\']+\.html)["\'][^>]*>', file_content, re.IGNORECASE)
    for link in links:
        yield link

# Define pipeline options for local execution
options = PipelineOptions()
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DirectRunner'

# Create the pipeline for local execution
pipeline = beam.Pipeline(options=options)

# Define the local file pattern
local_file_pattern = 'C:/Users/aishw/Documents/webdir/*.html'

# Reading and parsing files
parsed_files = (
    pipeline
    | 'MatchFiles' >> fileio.MatchFiles(local_file_pattern)
    | 'ReadMatches' >> fileio.ReadMatches()
    | 'ReadFiles' >> beam.Map(lambda x: (x.metadata.path, x.read_utf8()))
    | 'ParseFiles' >> beam.FlatMap(parse_file)
)

# Counting outgoing links
outgoing_counts = (
    parsed_files
    | 'CountOutgoingLinks' >> beam.combiners.Count.PerElement()
)

# Extracting and counting incoming links
incoming_counts = (
    parsed_files
    | 'MapToIncomingLink' >> beam.Map(lambda link: (link, 1))
    | 'GroupIncomingLinks' >> beam.GroupByKey()
    | 'CountUniqueIncomingLinks' >> beam.Map(lambda x: (x[0], len(set(x[1]))))
)

# Determine the top files with incoming and outgoing links
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
output_file_path = f"C:/Users/aishw/Documents/output_{timestamp}"

top_incoming = (
    incoming_counts
    | 'TopIncoming' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
    | 'WriteTopIncoming' >> beam.io.WriteToText(output_file_path + '_top_incoming')
)

top_outgoing = (
    outgoing_counts
    | 'TopOutgoing' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
    | 'WriteTopOutgoing' >> beam.io.WriteToText(output_file_path + '_top_outgoing')
)

# Execute the pipeline
result = pipeline.run()
result.wait_until_finish()
