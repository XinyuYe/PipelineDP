from dataclasses import dataclass
import typing

from typing import Iterable
import abc
import pipeline_dp
from pipeline_dp.aggregate_params import Metrics
from pipeline_dp.pipeline_operations import PipelineOperations
from pipeline_dp.accumulator import Accumulator, CompoundAccumulator


@dataclass
class SketchParams:
    metrics: Iterable[Metrics]
    partition_sampling_probability: float = 1
    number_of_sampled_partitions: int = -1


# @dataclass
# class TrueAggregateParams:
#     """Specifies parameters for function aggregate_true()

#   Args:
#     metrics: Metrics to compute.
#   """

#     metrics: Iterable[Metrics]

#     def __str__(self):
#         return f"Metrics: {[m.value for m in self.metrics]}"


class DataPeeker:
    """Generates sketches for privacy utility analysis"""

    def __init__(self, ops: PipelineOperations):
        self._ops = ops

    def sketch(self, col, params: SketchParams,
               data_extractors: pipeline_dp.DataExtractors):
        """Generates sketches in the format of (partition_key, value, partition_count).

        The sketches has one entry for each unique (partition_key, privacy_id).
        Parameter tuning on outputs of sketch ignores `low` and `max` of
        AggregateParams

        partition_key: the hashed version of the current partition key
        partition_value: the per privacy id per partition_key aggregated value
        partition_count: number of partitions this privacy id contributes to
        """
        accumulator_factory = CompoundAccumulatorFactory(params=params)
        aggregator_fn = accumulator_factory.create

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
        # col = self._ops.map_values(col, lambda l: sum(l), "")
        col = self._ops.map_values(col, aggregator_fn, "")
        # col : ((partition_key, privacy_id), accumulator))

        col = self._ops.map_tuple(
            col, lambda pk_pid, p_value: (pk_pid[1], (pk_pid[0], p_value)), "")
        # col : (privacy_id, (partition_key, aggregated_value))
        col = self._ops.group_by_key(col, "")
        col = self._ops.map_values(
            col, lambda lst: (len(set(i[0] for i in lst)), lst), "")
        # col : (privacy_id, (partition_count, [(partition_key, aggregated_value)]))

        col = self._ops.flat_map(
            col, lambda x: [(i[0], i[1].compute_metrics()[0], x[1][0])
                            for i in x[1][1]], "")
        # (partition_key, aggregated_value, partition_count)
        return col

    def sample(self, col, params: SketchParams,
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

    def aggregate_true(self, col, params: SketchParams,
                       data_extractors: pipeline_dp.DataExtractors):
        accumulator_factory = CompoundAccumulatorFactory(params=params)
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
        col = self._ops.map_values(col, aggregator_fn, "Apply aggregate_fn")
        # ((privacy_id, partition_key), aggregator)
        col = self._ops.map_tuple(col, lambda pid_pk, v: (pid_pk[1], v),
                                  "Drop privacy id")
        # col : (partition_key, accumulator)
        col = self._ops.reduce_accumulators_per_key(
            col, "Reduce accumulators per partition key")
        # col : (partition_key, accumulator)
        # Compute metrics.
        col = self._ops.map_values(col, lambda acc: acc.compute_metrics(),
                                   "Compute DP metrics")
        # col : (partition_key, aggregated_value)
        return col


class CountAccumulator(Accumulator):

    def __init__(self, values):
        self._count = len(values)

    def add_value(self, value):
        self._count += 1

    def add_accumulator(self,
                        accumulator: "CountAccumulator") -> "CountAccumulator":
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
                        accumulator: "SumAccumulator") -> "SumAccumulator":
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
        self, accumulator: "PrivacyIdCountAccumulator"
    ) -> "PrivacyIdCountAccumulator":
        self._check_mergeable(accumulator)
        self._count += accumulator._count
        return self

    def compute_metrics(self) -> float:
        return self._count


def _create_accumulator_factories(
    params: SketchParams,) -> typing.List["AccumulatorFactory"]:
    factories = []
    if pipeline_dp.Metrics.COUNT in params.metrics:
        factories.append(CountAccumulatorFactory())
    if pipeline_dp.Metrics.SUM in params.metrics:
        factories.append(SumAccumulatorFactory())
    if pipeline_dp.Metrics.PRIVACY_ID_COUNT in params.metrics:
        factories.append(PrivacyIdCountAccumulatorFactory())
    return factories


class AccumulatorFactory(abc.ABC):
    """Abstract base class for all accumulator factories.

    Each concrete implementation of AccumulatorFactory creates Accumulator of
    the specific type.
    """

    @abc.abstractmethod
    def create(self, values: typing.List) -> Accumulator:
        pass


class CountAccumulatorFactory(AccumulatorFactory):

    def create(self, values: typing.List) -> CountAccumulator:
        return CountAccumulator(values)


class SumAccumulatorFactory(AccumulatorFactory):

    def create(self, values: typing.List) -> SumAccumulator:
        return SumAccumulator(values)


class PrivacyIdCountAccumulatorFactory(AccumulatorFactory):

    def create(self, values: typing.List) -> PrivacyIdCountAccumulator:
        return PrivacyIdCountAccumulator(values)


class CompoundAccumulatorFactory(AccumulatorFactory):
    """Factory for creating CompoundAccumulator.

    CompoundAccumulatorFactory contains one or more AccumulatorFactories which
    create accumulators for specific metrics. These AccumulatorFactories are
    created based on pipeline_dp.AggregateParams.
    """

    def __init__(self, params: SketchParams):
        self._params = params
        self._accumulator_factories = _create_accumulator_factories(params)

    def create(self, values: typing.List) -> Accumulator:
        accumulators = []
        for factory in self._accumulator_factories:
            accumulators.append(factory.create(values))

        return CompoundAccumulator(accumulators)
