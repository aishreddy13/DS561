import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from bs4 import BeautifulSoup
from apache_beam.io import fileio

class ExtractOutgoingLinkCount(beam.DoFn):
    def process(self, element):
        soup = BeautifulSoup(element[1], 'html.parser')
        links = soup.find_all('a')
        yield (element[0], len(links))

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input", required=True, help="Input for HTML files")
        parser.add_argument("--output", required=True, help="Output for top link counts")

def run(argv=None):
    options = MyOptions(argv)

    with beam.Pipeline(options=options) as pipe:
        html_files = (pipe
                      | 'MatchFiles' >> fileio.MatchFiles(options.input)
                      | 'ReadMatches' >> fileio.ReadMatches()
                      | 'Converting to (Filename, HTML Content)' >> beam.Map(
                          lambda x: (x.metadata.path, x.read_utf8())))

        counting_out = (html_files
                        | 'Getting outgoing count' >> beam.ParDo(ExtractOutgoingLinkCount())
                        | 'Extracting top 5' >> beam.combiners.Top.Of(5, key=lambda x: x[1])
                        | 'Output results' >> beam.io.WriteToText(options.output, file_name_suffix=".txt"))

if __name__ == '__main__':
    run()
