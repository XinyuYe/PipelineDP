from absl import app

import pipeline_dp
from wally_sketch import data_peeker


def generate_sketches(col):
    data_extractor = pipeline_dp.DataExtractors(
        privacy_id_extractor=lambda x: f"pid{x}",
        partition_extractor=lambda x: f"pk{x%10}",
        value_extractor=lambda x: x)
    pipeline_operations = pipeline_dp.LocalPipelineOperations()
    peeker = data_peeker.DataPeeker(pipeline_operations)
    sample_params = data_peeker.SampleParams(number_of_sampled_partitions=5)
    print(list(peeker.sketch(col, sample_params, data_extractor)))


def main(unused_argv):
    del unused_argv  # unused
    input_data = [i for i in range(500)]
    generate_sketches(input_data)


if __name__ == '__main__':
    app.run(main)
