from __future__ import annotations

import networkx as nx

from fin_models.bar_handlers import BarHandler, History


def construct_bar_handlers(
    root: type[BarHandler],
    seed_instances: list[BarHandler] | None = None,
    _custom_scope: dict | None = None,
) -> list[BarHandler]:
    instances: dict[type[BarHandler], BarHandler] = {
        instance.__class__: instance for instance in (seed_instances or [])
    }

    dag = nx.DiGraph()

    def add_bh_to_dag(bh):
        if isinstance(bh, BarHandler) and bh.__class__ not in instances:
            instances[bh.__class__] = bh

        bar_handler_deps = bh.get_class_bar_handlers(_custom_scope=_custom_scope)

        if bh.import_path not in dag.nodes:
            if isinstance(bh, BarHandler):
                dag.add_node(bh.import_path, instance=bh)
            else:
                dag.add_node(bh.import_path, klass=bh, kw=bar_handler_deps)

        for bar_handler_dep in reversed(bar_handler_deps.values()):
            add_bh_to_dag(bar_handler_dep)
            dag.add_edge(bh.import_path, bar_handler_dep.import_path)

    add_bh_to_dag(root)

    try:
        order = list(reversed(list(nx.topological_sort(dag))))
    except nx.NetworkXUnfeasible:
        msg = "Circular dependency detected between bar handlers"
        problem_graph = ", ".join(f"{a} -> {b}" for a, b in nx.find_cycle(dag))
        raise Exception(f"{msg}: {problem_graph}")

    bar_handler_order: list[BarHandler] = []
    for node_name in order:
        if instance := dag.nodes[node_name].get("instance"):
            instances[instance.__class__] = instance
            bar_handler_order.append(instance)

        elif klass := dag.nodes[node_name].get("klass"):
            if klass not in instances:
                kwargs = {}
                for kw, dep_klass in dag.nodes[node_name].get("kw", {}).items():
                    if isinstance(dep_klass, BarHandler):
                        kwargs[kw] = instances[dep_klass.__class__] = dep_klass
                    elif instance := instances.get(dep_klass):
                        kwargs[kw] = instance
                    else:
                        kwargs[kw] = instances[dep_klass] = dep_klass()

                instances[klass] = klass(**kwargs)
            bar_handler_order.append(instances[klass])

    history: History
    if history := instances.get(History):
        freq_requests = history.freq_requests or {}
        for bh in bar_handler_order:
            if freqs := getattr(bh, "history_reqs", {}):
                for freq, num_bars in freqs.items():
                    freq_requests[freq] = (
                        max(freq_requests.get(freq, 0) or 0, num_bars or 0) or None
                    )
        history.freq_requests = freq_requests

    return bar_handler_order
