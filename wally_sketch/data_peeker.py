from dataclasses import dataclass
import typing

import pipeline_dp
from pipeline_dp.aggregate_params import AggregateParams
from pipeline_dp.budget_accounting import BudgetAccountant
from pipeline_dp.pipeline_operations import LocalPipelineOperations, PipelineOperations
from pipeline_dp.accumulator import Accumulator, AccumulatorFactory, AccumulatorParams, CompoundAccumulator


@dataclass
class SampleParams:
    partition_sampling_probability: float = 1
    number_of_sampled_partitions: int = -1


class DataPeeker:
    """Generates sketches for privacy utility analysis"""

    def __init__(self, ops: LocalPipelineOperations):
        self._ops = ops

    def sketch(self, col, params: SampleParams,
               data_extractors: pipeline_dp.DataExtractors):
        """Generates sketches in the format of (partition_key, value, partition_count).
        The sketches has one entry for each unique (partition_key, privacy_id).

        partition_key: the hashed version of the current partition key
        partition_value: the per privacy id per partition_key aggregated value
        partition_count: number of partitions this privacy id contributes to
        """
        # Extract the columns.
        col = self._ops.map(
            col, lambda row: (data_extractors.privacy_id_extractor(row),
                              data_extractors.partition_extractor(row),
                              data_extractors.value_extractor(row)),
            "Extract (privacy_id, partition_key, value))")
        # col : (privacy_id, partition_key, value)
        col = self._ops.map_tuple(
            col, lambda pid, pk, v: (pk, (pid, v)),
            "Rekey to (partition_key, (privacy_id, value))")
        # col : (partition_key, (privacy_id, value))
        # sample
        # group by key, filter keys by sampling, expand the values by flat map
        # TODO(b/): consider using reduce_accumulators_per_key
        col = self._ops.group_by_key(col, "Group by pk")
        col = self._ops.map_tuple(col, lambda pk, pid_v_seq: (1,
                                                              (pk, pid_v_seq)),
                                  "Rekey to (1, (pk, pid_v_seq))")
        col = self._ops.sample_fixed_per_key(
            col, params.number_of_sampled_partitions, "Sample partitions")
        col = self._ops.flat_map(col, lambda plst: plst[1], "")
        col = self._ops.flat_map(col, lambda x: [(x[0], i) for i in x[1]],
                                 "Transform to (pkey, (pid, value))")

        # TODO(b/): Decide calculates partition_count after sampling or not
        # col : (partition_key, (privacy_id, value))
        # calculates partition_count after sampling and per (partition_key, privacy_id) pair aggregated value

        col = self._ops.map_tuple(col, lambda pk, pid_v:
                                  ((pk, pid_v[0]), pid_v[1]), "")
        # col : ((partition_key, privacy_id), value))
        col = self._ops.group_by_key(col, "")
        col = self._ops.map_values(col, lambda lst: sum(lst), "")
        # col : ((partition_key, privacy_id), aggregated_value))

        col = self._ops.map_tuple(
            col, lambda pk_pid, p_value: (pk_pid[1], (pk_pid[0], p_value)), "")
        # col : (privacy_id, (partition_key, aggregated_value))
        col = self._ops.group_by_key(col, "")
        col = self._ops.map_values(
            col, lambda lst: (len(set(i[0] for i in lst)), lst), "")
        # col : (privacy_id, (partition_count, [(partition_key, aggregated_value)]))

        col = self._ops.flat_map(
            col, lambda x: [(i[0], i[1], x[1][0]) for i in x[1][1]], "")
        # (partition_key, value, partition_count)
        return col

    def sample(self, col, params: SampleParams,
               data_extractors: pipeline_dp.DataExtractors):
        # Extract the columns.
        col = self._ops.map(
            col, lambda row: (data_extractors.privacy_id_extractor(row),
                              data_extractors.partition_extractor(row),
                              data_extractors.value_extractor(row)),
            "Extract (privacy_id, partition_key, value))")
        # col : (privacy_id, partition_key, value)
        col = self._ops.map_tuple(
            col, lambda pid, pk, v: (pk, (pid, v)),
            "Rekey to (partition_key, (privacy_id, value))")
        # col : (partition_key, (privacy_id, value))
        # sample
        # group by key, filter keys by sampling, expand the values by flat map
        # TODO(b/): consider using reduce_accumulators_per_key
        col = self._ops.group_by_key(col, "Group by pk")
        col = self._ops.map_tuple(col, lambda pk, pid_v_seq: (1,
                                                              (pk, pid_v_seq)),
                                  "Rekey to (1, (pk, pid_v_seq))")
        col = self._ops.sample_fixed_per_key(
            col, params.number_of_sampled_partitions, "Sample partitions")
        col = self._ops.flat_map(col, lambda plst: plst[1], "")
        col = self._ops.flat_map(
            col, lambda pk_pidVSeq: [(pid, pk_pidVSeq[0], v)
                                     for pid, v in pk_pidVSeq[1]],
            "Transform to (pid, pk, value)")
        return col

    def aggregate_true(self, col, params: AggregateParams,
                       data_extractors: pipeline_dp.DataExtractors):
        accumulator_factory = AccumulatorFactory(params=params)
        accumulator_factory.initialize()
        aggregator_fn = accumulator_factory.create

        col = self._ops.map(
            col, lambda row: (data_extractors.privacy_id_extractor(row),
                              data_extractors.partition_extractor(row),
                              data_extractors.value_extractor(row)),
            "Extract (privacy_id, partition_key, value))")
        # col : (privacy_id, partition_key, value)
        col = self._ops.map_tuple(
            col, lambda pid, pk, v: ((pid, pk), v),
            "Rekey to ( (privacy_id, partition_key), value))")
        col = self._ops.group_by_key(col, "Group by pk")
        # ((privacy_id, partition_key), [value])
        col = self._ops.map_values(
            col, aggregator_fn,
            "Apply aggregate_fn after per partition bounding")
        # ((privacy_id, partition_key), aggregator)

        col = self._ops.map_tuple(col, lambda pid_pk, v: (pid_pk[1], v),
                                  "Drop privacy id")
        # col : (partition_key, accumulator)
        col = self._ops.reduce_accumulators_per_key(
            col, "Reduce accumulators per partition key")
        # col : (partition_key, accumulator)
        # Compute metrics.
        col = self._ops.map_values(col, lambda acc: acc.compute_metrics(),
                                   "Compute DP` metrics")
        return col


class CountAccumulator(Accumulator):

    def __init__(self, values):
        self._count = len(values)

    def add_value(self, value):
        self._count += 1

    def add_accumulator(self,
                        accumulator: 'CountAccumulator') -> 'CountAccumulator':
        self._check_mergeable(accumulator)
        self._count += accumulator._count
        return self

    def compute_metrics(self) -> float:
        return self._count


class SumAccumulator(Accumulator):

    def __init__(self, values):
        self._sum = sum(values)

    def add_value(self, value):
        self._sum += value

    def add_accumulator(self,
                        accumulator: 'SumAccumulator') -> 'SumAccumulator':
        self._check_mergeable(accumulator)
        self._sum += accumulator._sum

    def compute_metrics(self) -> float:
        return self._sum


class PrivacyIdCountAccumulator(Accumulator):

    def __init__(self, values):
        self._count = 1

    def add_value(self, value):
        pass

    def add_accumulator(
        self, accumulator: 'PrivacyIdCountAccumulator'
    ) -> 'PrivacyIdCountAccumulator':
        self._check_mergeable(accumulator)
        self._count += accumulator._count
        return self

    def compute_metrics(self) -> float:
        return self._count


def create_accumulator_params(aggregation_params: AggregateParams):
    accumulator_params = []
    if pipeline_dp.Metrics.COUNT in aggregation_params.metrics:
        accumulator_params.append(
            AccumulatorParams(accumulator_type=CountAccumulator,
                              constructor_params=None))
    if pipeline_dp.Metrics.SUM in aggregation_params.metrics:
        accumulator_params.append(
            AccumulatorParams(accumulator_type=SumAccumulator,
                              constructor_params=None))
    if pipeline_dp.Metrics.PRIVACY_ID_COUNT in aggregation_params.metrics:
        accumulator_params.append(
            AccumulatorParams(accumulator_type=PrivacyIdCountAccumulator,
                              constructor_params=None))
    return accumulator_params


class AccumulatorFactory:
    """Factory for producing the appropriate Accumulator depending on the
    AggregateParams and BudgetAccountant."""

    def __init__(self, params: pipeline_dp.AggregateParams):
        self._params = params

    def initialize(self):
        self._accumulator_params = create_accumulator_params(self._params)

    def create(self, values: typing.List) -> Accumulator:
        accumulators = []
        for accumulator_param in self._accumulator_params:
            accumulators.append(accumulator_param.accumulator_type(values))

        return CompoundAccumulator(accumulators)
