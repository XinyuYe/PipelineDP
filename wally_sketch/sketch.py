from absl import app

import pipeline_dp
from wally_sketch import data_peeker, peeker_dp_engine
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Sketch based methods supports a single aggregatione metric. The metic must be
# sum and count.
def generate_sketches(col):
    data_extractor = pipeline_dp.DataExtractors(
        privacy_id_extractor=lambda x: f"pid{x}",
        partition_extractor=lambda x: f"pk{x%10}",
        value_extractor=lambda x: x)
    pipeline_operations = pipeline_dp.BeamOperations()
    peeker = data_peeker.DataPeeker(pipeline_operations)
    # Only supports a single metrics for now.
    metrics = [pipeline_dp.Metrics.SUM]
    sample_params = data_peeker.SketchParams(metrics=metrics,
                                             number_of_sampled_partitions=5)
    # print(list(peeker.sketch(col, sample_params, data_extractor)))
    sketches = peeker.sketch(col, sample_params, data_extractor)
    return sketches


def materialize_sketches(sketches):
    return list(sketches)


def dp_sketches(sketches):
    # The following should happen in realtime
    pipeline_operations = pipeline_dp.LocalPipelineOperations()
    budget_accountant = pipeline_dp.NaiveBudgetAccountant(total_epsilon=3,
                                                          total_delta=1e-5)
    dp_engine = peeker_dp_engine.SketchDPEngine(budget_accountant,
                                                pipeline_operations)
    params = pipeline_dp.AggregateParams(
        noise_kind=pipeline_dp.NoiseKind.LAPLACE,
        metrics=[pipeline_dp.Metrics.SUM],
        max_partitions_contributed=1,
        max_contributions_per_partition=10,
        low=0,
        high=1)
    dp_results = dp_engine.aggregate_sketches(sketches, params)
    budget_accountant.compute_budgets()
    print(list(dp_results))
    # print(dp_engine._report_generators[0].report())


def main(unused_argv):
    del unused_argv  # unused
    input_data = [i for i in range(500)]
    # sketches = generate_sketches(input_data)
    file_prefix = ''
    # sketches | 'To string' >> beam.Map(lambda s: ','.join(
    #     str(i) for i in s)) | 'Write to files' >> beam.io.WriteToText(
    #         file_path_prefix=file_prefix, file_name_suffix='.txt')
    read_sketches = []
    with open(file_prefix + '-00000-of-00001.txt') as f:
        for line in f:
            pk, pv, pcount = line.strip().split(',')
            read_sketches.append((pk, int(pv), int(pcount)))

    # options = PipelineOptions(flags=[], type_check_additional='all')
    # read_sketches = []
    # with beam.Pipeline(options=options) as pipeline:
    #     pipeline | beam.io.ReadFromText(file_pattern=file_path) | beam.Map(
    #         lambda l: read_sketches.append(l.split(',')))
    # print(read_sketches)
    # print(list(read_skethces))
    # sketches = materialize_sketches(read_sketches)
    # print(sketches)
    dp_sketches(read_sketches)


if __name__ == '__main__':
    app.run(main)
