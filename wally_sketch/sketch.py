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


def diff_metrics(raw_result, dp_result):
    # Assume both to be in the format of [(pk, [orderred metrics])]
    raw_dict = dict(raw_result)
    dp_dict = dict(dp_result)
    print(f'raw partitions: ', len(raw_dict))
    print(f'dp partitions: ', len(dp_dict))
    print(f'dropped partition: ', len(raw_dict) - len(dp_dict))
    print('Difference:')
    for k, raw_metrics in raw_dict.items():
        if k in dp_dict:
            print(k, [
                f'{(dp_v - raw_v) / raw_v:%}'
                for dp_v, raw_v in zip(dp_dict[k], raw_metrics)
            ])
        else:
            print(k, ' N/A')


def main(unused_argv):
    del unused_argv  # unused
    # sampling data: (pid, pk, value)
    MAX_PARTITION_NUM = 10
    input_data = [i for i in range(500)]
    data_extractor = pipeline_dp.DataExtractors(
        privacy_id_extractor=lambda x: f"pid{x}",
        partition_extractor=lambda x: f"pk{x%10}",
        value_extractor=lambda x: x)
    pipeline_operations = pipeline_dp.LocalPipelineOperations()
    peeker = data_peeker.DataPeeker(pipeline_operations)
    sample_params = data_peeker.SampleParams(
        number_of_sampled_partitions=MAX_PARTITION_NUM)
    # TODO: Materiaze for next steps, figure out other methods than materialization
    sampled_data = list(peeker.sample(input_data, sample_params,
                                      data_extractor))
    # sampled_data: (pid, pk, value)

    # Begin normal DP pipelines
    dummy_extractor = pipeline_dp.DataExtractors(
        privacy_id_extractor=lambda x: x[0],
        partition_extractor=lambda x: x[1],
        value_extractor=lambda x: x[2])
    budget_accountant = pipeline_dp.NaiveBudgetAccountant(total_epsilon=3,
                                                          total_delta=1e-5)
    dp_engine = pipeline_dp.DPEngine(budget_accountant, pipeline_operations)
    params = pipeline_dp.AggregateParams(
        noise_kind=pipeline_dp.NoiseKind.LAPLACE,
        metrics=[pipeline_dp.Metrics.COUNT, pipeline_dp.Metrics.SUM],
        max_partitions_contributed=2,
        max_contributions_per_partition=10,
        low=0,
        high=1)
    dp_result = dp_engine.aggregate(sampled_data, params, dummy_extractor)
    budget_accountant.compute_budgets()
    dp_result = list(dp_result)
    raw_result = list(
        peeker.aggregate_true(sampled_data, params, dummy_extractor))
    diff_metrics(raw_result, dp_result)
    return 0


if __name__ == '__main__':
    app.run(main)
