# Infra Context

`infra/` contains shared infrastructure used by many modules.

Use this directory when the task depends on shared plumbing such as transport, shared memory, storage backends, logging, tracing, or metrics.

Current docs:

- `common-infra.md`: broad inventory of common building blocks.
- `logging/README.md`: logging-area overview and routing page.
- `logging/design.md`: logging architecture, feature-extension points, and compatibility guardrails.
- `logging/trace-and-context.md`: thread-local trace state and propagation.
- `logging/access-recorder.md`: access/performance record emission and key mapping.
- `logging/log-lifecycle-and-rotation.md`: startup, rotation, flush, and failure logging.
- `metrics/README.md`: metrics-area overview and routing page.
- `metrics/design.md`: metrics architecture, registration model, and compatibility guardrails.
- `metrics/resource-collector.md`: collector lifecycle, handler registration, and interval-driven sampling.
- `metrics/exporters-and-buffering.md`: exporter buffering, flush threads, and hard-disk output.
- `metrics/metric-families-and-registration.md`: metric definitions, descriptions, and runtime registration points.
