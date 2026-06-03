def build_dag_definition():
    return ["load_user_events", "aggregate_behavior", "export_behavior_graph"]
