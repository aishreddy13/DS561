import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Define a Beam transform to count occurrences of "This is a link"
class CountOutgoingLinksFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        # element is a tuple containing the file name and its content
        file_name, content = element
        # Count occurrences of "This is a link"
        count = content.count("This is a link")
        yield file_name, count

# Define pipeline options, such as runner type
pipeline_options = PipelineOptions(flags=[])

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    counts = (
        p | 'ReadFiles' >> beam.io.textio.ReadFromText('C:/Users/aishw/Documents/0.html')
          | 'CountLinks' >> beam.ParDo(CountOutgoingLinksFn())
          | 'FormatResults' >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
          | 'WriteResults' >> beam.io.textio.WriteToText('C:/Users/aishw/Documents/output.txt')
    )
