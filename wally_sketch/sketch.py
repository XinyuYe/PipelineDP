from absl import app

import pipeline_dp
from wally_sketch import data_peeker, peeker_dp_engine


# Sketch based methods supports a single aggregatione metric. The metic must be
#  sum and count.
def generate_sketches(col):
    data_extractor = pipeline_dp.DataExtractors(
        privacy_id_extractor=lambda x: f"pid{x}",
        partition_extractor=lambda x: f"pk{x%10}",
        value_extractor=lambda x: x)
    pipeline_operations = pipeline_dp.LocalPipelineOperations()
    peeker = data_peeker.DataPeeker(pipeline_operations)
    # Only supports a single metrics for now.
    metrics = [pipeline_dp.Metrics.SUM]
    sample_params = data_peeker.SketchParams(metrics=metrics,
                                             number_of_sampled_partitions=5)
    # print(list(peeker.sketch(col, sample_params, data_extractor)))
    col = peeker.sketch(col, sample_params, data_extractor)

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
    dp_results = dp_engine.aggregate_sketches(col, params)
    budget_accountant.compute_budgets()
    print(list(dp_results))
    # print(dp_engine._report_generators[0].report())


def main(unused_argv):
    del unused_argv  # unused
    input_data = [i for i in range(500)]
    generate_sketches(input_data)


if __name__ == '__main__':
    app.run(main)
