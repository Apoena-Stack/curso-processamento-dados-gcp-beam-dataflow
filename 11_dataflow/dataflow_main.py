import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipe_options_dict = {
    "project": "dataflow-example-435615",
    "runner": "DataflowRunner",
    "region": "us-east1",
    "staging_location": "gs://dataflow-exemplo-apoena/temp",
    "temp_location": "gs://dataflow-exemplo-apoena/temp",
    "template_location": "gs://dataflow-exemplo-apoena/models/template_01"
}

pipeline_options = PipelineOptions.from_dictionary(pipe_options_dict)
p1 = beam.Pipeline(options=pipeline_options)

input_file = "gs://dataflow-exemplo-apoena/input/top10.txt"
output_file = "gs://dataflow-exemplo-apoena/output/output.txt"

top5 = (
    p1
    | beam.io.ReadFromText(input_file, skip_header_lines=1)
    | beam.Map(lambda record: record.split(","))
    | beam.Filter(lambda record: int(record[0]) <= 5)
    | beam.io.WriteToText(output_file)
)

p1.run()