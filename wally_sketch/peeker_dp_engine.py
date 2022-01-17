import pipeline_dp
from pipeline_dp.aggregate_params import AggregateParams
from pipeline_dp.pipeline_operations import PipelineOperations
from pipeline_dp import BudgetAccountant
from typing import Any, Sequence, Tuple
from pipeline_dp.accumulator import CompoundAccumulatorFactory
import numpy as np
from functools import partial
from pipeline_dp.aggregate_params import MechanismType
from pipeline_dp.budget_accounting import BudgetAccountant, MechanismSpec
import pydp.algorithms.partition_selection as partition_selection


def aggregate_sketch_true(ops: PipelineOperations, col,
                          metric: pipeline_dp.Metrics):
    if metric == pipeline_dp.Metrics.SUM:
        aggregator_fn = sum
    elif metric == pipeline_dp.Metrics.COUNT:
        aggregator_fn = len
    else:
        raise ValueError('Aggregate sketch only supports sum or count')
    # col: (partition_key, per_user_aggregated_value, partition_count)
    col = ops.map_tuple(col, lambda pk, pval, _: (pk, pval),
                        'Drop partition count')
    # col: (partition_key, per_user_aggregated_value)
    col = ops.group_by_key(col, "Group by partition key")
    # col: (partition_key, [per_user_aggregated_value])
    col = ops.map_values(col, lambda val: aggregator_fn(val),
                         "Aggregate by partition key")
    return col


class SketchDPEngine:
    """Performs DP aggregations."""

    def __init__(self, budget_accountant: BudgetAccountant,
                 ops: PipelineOperations):
        self._budget_accountant = budget_accountant
        self._ops = ops
        self._report_generators = []

    def aggregate_sketches_dp(self, col, params: AggregateParams):
        # col: (partition_key, per_user_aggregated_value, partition_count)
        # self._report_generators.append(ReportGenerator(params))
        accumulator_factory = CompoundAccumulatorFactory(
            params=params, budget_accountant=self._budget_accountant)
        aggregator_fn = accumulator_factory.create

        def filter_fn(max_partitions: int, col: Tuple[Any, int, int]) -> bool:
            if col[1] <= max_partitions:
                return True
            return np.random.rand() < max_partitions / col[2]

        col = self._ops.filter(
            col, partial(filter_fn, params.max_partitions_contributed),
            "Cross partition bounding")
        col = self._ops.map_tuple(
            col, lambda pk, pval, _:
            (pk, params.max_contributions_per_partition
             if pval > params.max_contributions_per_partition else pval),
            "Per partition bounding")
        # col: (partition_key, per_user_aggregated_value)
        col = self._ops.group_by_key(col, "Group by partition key")
        # col: (partition_key, [per_user_aggregated_value])
        # Partition selection
        budget = self._budget_accountant.request_budget(
            mechanism_type=MechanismType.GENERIC)

        def filter_fn_partition_selection(
                captures: Tuple[MechanismSpec,
                                int], row: Tuple[Any, Sequence[int]]) -> bool:
            budget, max_partitions = captures
            values = row[1]
            partition_selection_strategy = partition_selection.create_truncated_geometric_partition_strategy(
                budget.eps, budget.delta, max_partitions)
            return partition_selection_strategy.should_keep(len(values))

        filter_fn_partition_selection = partial(
            filter_fn_partition_selection,
            (budget, params.max_partitions_contributed))

        # self._add_report_stage(
        #     lambda:
        #     f"Private Partition selection: using {budget.mechanism_type.value} "
        #     f"method with (eps= {budget.eps}, delta = {budget.delta})")

        col = self._ops.filter(col, filter_fn_partition_selection,
                               "Filter private partitions")
        col = self._ops.map_values(
            col, lambda val: aggregator_fn(val).compute_metrics(),
            "Aggregate by partition key")
        return col
