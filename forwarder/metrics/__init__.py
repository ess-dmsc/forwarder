"""
Classes to model monitoring metrics based on the Prometheus client library.

Since we push these metrics to Graphite instead of relying on Prometheus
standard mechanisms, we provide adapter classes to prevent coupling other
modules with certain Prometheus internals.
"""

import re

from prometheus_client import Counter as PrometheusCounter
from prometheus_client import Gauge as PrometheusGauge
from prometheus_client import Summary as PrometheusSummary


def sanitise_metric_name(metric_name):
    return re.sub(r"[^a-zA-Z0-9_:]", "_", metric_name)


class Gauge(PrometheusGauge):
    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value.get()


class Counter(PrometheusCounter):
    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value.get()


class Summary(PrometheusSummary):
    @property
    def name(self):
        return self._name

    @property
    def count(self):
        return self._count.get()

    @property
    def sum(self):
        return self._sum.get()
