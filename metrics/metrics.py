import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
from pydantic import BaseModel
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from metrics.deployments import PrefectDeployments
from metrics.flow_runs import PrefectFlowRuns
from metrics.flows import PrefectFlows
from metrics.work_pools import PrefectWorkPools
from metrics.work_queues import PrefectWorkQueues

TERMINAL_STATES = frozenset({"Completed", "Failed", "Crashed", "Cancelled"})


class CsrfToken(BaseModel):
    token: str
    expiration: datetime


class PrefectMetrics(object):
    """
    PrefectMetrics class for collecting and exposing Prometheus metrics related to Prefect.
    """

    def __init__(
        self,
        url,
        headers,
        offset_minutes,
        failed_runs_offset_minutes,
        failed_runs_limit,
        max_retries,
        csrf_enabled,
        client_id,
        logger,
        enable_pagination,
        pagination_limit,
    ) -> None:
        """
        Initialize the PrefectMetrics instance.

        Args:
            url (str): The URL of the Prefect instance.
            headers (dict): Headers to be included in HTTP requests.
            offset_minutes (int): Time offset in minutes.
            failed_runs_offset_minutes (int): Time offset in minutes for failed runs lookup window.
            failed_runs_limit (int): Maximum number of last failed runs to report per deployment.
            max_retries (int): The maximum number of retries for HTTP requests.
            logger (obj): The logger object.
            csrf_enabled (bool): Whether CSRF is enabled.
            client_id (str): The client ID for CSRF.
            enable_pagination (bool): Whether pagination is enabled.
            pagination_limit (int): The pagination limit.
        """
        self.headers = headers
        self.offset_minutes = offset_minutes
        self.failed_runs_offset_minutes = failed_runs_offset_minutes
        self.failed_runs_limit = failed_runs_limit
        self.url = url
        self.max_retries = max_retries
        self.logger = logger
        self.client_id = client_id
        self.csrf_enabled = csrf_enabled
        self.enable_pagination = enable_pagination
        self.pagination_limit = pagination_limit
        self.csrf_token = None
        self.csrf_token_expiration = None

        # persistent state for the finished-runs counter
        self._seen_finished_ids = set()
        self._finished_by_state = defaultdict(int)

    def collect(self):
        """
        Collect all Prefect metrics for a single Prometheus scrape.

        On failure, logs the error and yields no metrics. The exporter stays
        alive so subsequent scrapes can succeed.
        """
        try:
            yield from self._collect_metrics()
        except Exception:
            self.logger.exception("Failed to collect metrics, skipping this scrape")

    def _collect_metrics(self):
        """
        Internal method that performs the actual metric collection.
        """
        ##
        # PREFECT GET CSRF TOKEN IF ENABLED
        #
        if self.csrf_enabled:
            if not self.csrf_token or (
                self.csrf_token_expiration is not None
                and datetime.now(timezone.utc) > self.csrf_token_expiration
            ):
                self.logger.info(
                    "CSRF Token is expired or has not been generated yet. Fetching new CSRF Token..."
                )
                token_information = self.get_csrf_token()
                self.csrf_token = token_information.token
                self.csrf_token_expiration = token_information.expiration
            self.headers["Prefect-Csrf-Token"] = self.csrf_token
            self.headers["Prefect-Csrf-Client"] = self.client_id

        ##
        # PREFECT GET RESOURCES
        #
        deployments = PrefectDeployments(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_deployments_info()
        flows = PrefectFlows(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flows_info()
        flow_runs = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flow_runs_info()
        all_flow_runs = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_all_flow_runs_info()
        if self.failed_runs_offset_minutes == 0:
            failed_flow_runs = {}
        else:
            failed_flow_runs = PrefectFlowRuns(
                self.url,
                self.headers,
                self.max_retries,
                self.failed_runs_offset_minutes,
                self.logger,
                self.enable_pagination,
                self.pagination_limit,
            ).get_failed_flow_runs_info(limit=self.failed_runs_limit)
        work_pools = PrefectWorkPools(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_pools_info()
        work_queues = PrefectWorkQueues(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_queues_info()

        # ── lookup dicts (avoid O(n*m) scans) ──
        flows_by_id = {f["id"]: f["name"] for f in flows if f.get("id")}
        deployments_by_id = {d["id"]: d["name"] for d in deployments if d.get("id")}

        def _flow_name(obj):
            return str(flows_by_id.get(obj.get("flow_id"), "null"))

        def _deployment_name(obj):
            return str(deployments_by_id.get(obj.get("deployment_id"), "null"))

        ##
        # PREFECT DEPLOYMENTS METRICS
        #

        prefect_deployments = GaugeMetricFamily(
            "prefect_deployments_total", "Prefect total deployments", labels=[]
        )
        prefect_deployments.add_metric([], len(deployments))
        yield prefect_deployments

        prefect_info_deployments = GaugeMetricFamily(
            "prefect_info_deployment",
            "Prefect deployment info",
            labels=[
                "flow_name",
                "is_schedule_active",
                "deployment_name",
                "path",
                "paused",
                "work_pool_name",
                "work_queue_name",
                "status",
                "tags",
            ],
        )

        for deployment in deployments:
            flow_name = _flow_name(deployment)

            is_schedule_active = deployment.get("paused", "null")
            if is_schedule_active != "null":
                is_schedule_active = not is_schedule_active

            tags = deployment.get("tags", "null")
            if tags != "null":
                tags = ",".join(sorted(tags))

            prefect_info_deployments.add_metric(
                [
                    str(flow_name),
                    str(is_schedule_active),
                    str(deployment.get("name", "null")),
                    str(deployment.get("path", "null")),
                    str(deployment.get("paused", "null")),
                    str(deployment.get("work_pool_name", "null")),
                    str(deployment.get("work_queue_name", "null")),
                    str(deployment.get("status", "null")),
                    tags,
                ],
                1,
            )

        yield prefect_info_deployments

        ##
        # PREFECT FLOWS METRICS
        #

        prefect_flows = GaugeMetricFamily(
            "prefect_flows_total", "Prefect total flows", labels=[]
        )
        prefect_flows.add_metric([], len(flows))
        yield prefect_flows

        prefect_info_flows = GaugeMetricFamily(
            "prefect_info_flows",
            "Prefect flow info",
            labels=["flow_name"],
        )

        for flow in flows:
            prefect_info_flows.add_metric(
                [str(flow.get("name", "null"))],
                1,
            )

        yield prefect_info_flows

        ##
        # PREFECT FLOW RUNS METRICS
        #

        prefect_flow_runs = GaugeMetricFamily(
            "prefect_flow_runs_total", "Prefect total flow runs", labels=[]
        )
        prefect_flow_runs.add_metric([], len(all_flow_runs))
        yield prefect_flow_runs

        # ── finished-runs counter (monotonically increasing, survives across scrapes) ──
        current_all_ids = {run["id"] for run in all_flow_runs if run.get("id")}
        for run in all_flow_runs:
            run_id = run.get("id")
            state = run.get("state_name")
            if run_id and state in TERMINAL_STATES and run_id not in self._seen_finished_ids:
                self._seen_finished_ids.add(run_id)
                self._finished_by_state[state] += 1

        # prune IDs that aged out of the offset window
        self._seen_finished_ids &= current_all_ids

        prefect_finished_runs = CounterMetricFamily(
            "prefect_flow_runs_finished_total",
            "Monotonic count of finished flow runs by terminal state (use increase() or rate())",
            labels=["state"],
        )
        for state, count in self._finished_by_state.items():
            prefect_finished_runs.add_metric([state], count)
        yield prefect_finished_runs

        prefect_flow_runs_total_run_time = GaugeMetricFamily(
            "prefect_flow_runs_total_run_time",
            "Prefect flow-run total run time in seconds",
            labels=["flow_name"],
        )

        for flow_run in all_flow_runs:
            prefect_flow_runs_total_run_time.add_metric(
                [
                    _flow_name(flow_run),
                    str(flow_run.get("name", "null")),
                ],
                flow_run.get("total_run_time", "null"),
            )

        yield prefect_flow_runs_total_run_time

        # prefect_info_flow_runs — counts by state
        prefect_info_flow_runs = GaugeMetricFamily(
            "prefect_info_flow_runs",
            "Prefect flow runs info",
            labels=[
                "deployment_name",
                "flow_name",
                "state_name",
                "work_queue_name",
            ],
        )

        state_counts = defaultdict(int)

        for flow_run in flow_runs:
            label_key = (
                _deployment_name(flow_run),
                _flow_name(flow_run),
                str(flow_run.get("state_name", "null")),
                str(flow_run.get("work_queue_name", "null")),
            )
            state_counts[label_key] += 1

        for label_key, count in state_counts.items():
            prefect_info_flow_runs.add_metric(list(label_key), count)

        yield prefect_info_flow_runs

        # prefect_deployment_failed_flow_runs
        prefect_deployment_failed_flow_runs = GaugeMetricFamily(
            "prefect_deployment_failed_flow_runs",
            "Last failed flow run ID per deployment within the FAILED_RUNS_OFFSET_MINUTES window",
            labels=["deployment_name", "flow_name", "last_failed_run_id"],
        )

        for (deployment_id, flow_id), run_ids in failed_flow_runs.items():
            dep_name = deployments_by_id.get(deployment_id, "null")
            fl_name = flows_by_id.get(flow_id, "null")
            for run_id in run_ids:
                prefect_deployment_failed_flow_runs.add_metric(
                    [dep_name, fl_name, run_id], 1
                )

        yield prefect_deployment_failed_flow_runs

        ##
        # PREFECT WORK POOLS METRICS
        #

        prefect_work_pools = GaugeMetricFamily(
            "prefect_work_pools_total", "Prefect total work pools", labels=[]
        )
        prefect_work_pools.add_metric([], len(work_pools))
        yield prefect_work_pools

        prefect_info_work_pools = GaugeMetricFamily(
            "prefect_info_work_pools",
            "Prefect work pools info",
            labels=[
                "is_paused",
                "work_pool_name",
                "type",
                "status",
            ],
        )

        for work_pool in work_pools:
            state = 0 if work_pool.get("is_paused") else 1
            prefect_info_work_pools.add_metric(
                [
                    str(work_pool.get("is_paused", "null")),
                    str(work_pool.get("name", "null")),
                    str(work_pool.get("type", "null")),
                    str(work_pool.get("status", "null")),
                ],
                state,
            )

        yield prefect_info_work_pools

        ##
        # PREFECT WORK QUEUES METRICS
        #

        prefect_work_queues = GaugeMetricFamily(
            "prefect_work_queues_total", "Prefect total work queues", labels=[]
        )
        prefect_work_queues.add_metric([], len(work_queues))
        yield prefect_work_queues

        # late_runs_count as a proper numeric gauge (not buried in a label)
        prefect_late_runs = GaugeMetricFamily(
            "prefect_late_runs",
            "Number of late flow runs per work queue",
            labels=["work_pool_name", "work_queue_name"],
        )

        prefect_info_work_queues = GaugeMetricFamily(
            "prefect_info_work_queues",
            "Prefect work queues info",
            labels=[
                "is_paused",
                "work_queue_name",
                "priority",
                "type",
                "work_pool_name",
                "status",
                "healthy",
                "late_runs_count",
                "last_polled",
                "health_check_policy_maximum_late_runs",
                "health_check_policy_maximum_seconds_since_last_polled",
            ],
        )

        for work_queue in work_queues:
            state = 0 if work_queue.get("is_paused") else 1
            status_info = work_queue.get("status_info", {})
            health_check_policy = status_info.get("health_check_policy", {})

            late_runs_count = status_info.get("late_runs_count", 0)
            try:
                late_runs_count = int(late_runs_count)
            except (TypeError, ValueError):
                late_runs_count = 0

            prefect_late_runs.add_metric(
                [
                    str(work_queue.get("work_pool_name", "null")),
                    str(work_queue.get("name", "null")),
                ],
                late_runs_count,
            )

            prefect_info_work_queues.add_metric(
                [
                    str(work_queue.get("is_paused", "null")),
                    str(work_queue.get("name", "null")),
                    str(work_queue.get("priority", "null")),
                    str(work_queue.get("type", "null")),
                    str(work_queue.get("work_pool_name", "null")),
                    str(work_queue.get("status", "null")),
                    str(status_info.get("healthy", "null")),
                    str(status_info.get("late_runs_count", "null")),
                    str(status_info.get("last_polled", "null")),
                    str(health_check_policy.get("maximum_late_runs", "null")),
                    str(
                        health_check_policy.get(
                            "maximum_seconds_since_last_polled", "null"
                        )
                    ),
                ],
                state,
            )

        yield prefect_late_runs
        yield prefect_info_work_queues

    def get_csrf_token(self) -> CsrfToken:
        """
        Pull CSRF Token from CSRF Endpoint.

        Raises:
            requests.exceptions.RequestException: If all retries are exhausted.
        """
        endpoint = f"{self.url}/csrf-token?client={self.client_id}"

        for retry in range(self.max_retries):
            try:
                resp = requests.get(endpoint, headers=self.headers)
                resp.raise_for_status()
                return CsrfToken.model_validate(resp.json())
            except requests.exceptions.RequestException as err:
                self.logger.error(err)
                if retry < self.max_retries - 1:
                    time.sleep(2**retry)
                else:
                    raise
