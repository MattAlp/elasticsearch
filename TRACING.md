# Tracing in Elasticsearch

Elasticsearch is instrumented using the [OpenTelemetry][otel] API, which allows
ES developers to gather traces and analyze what Elasticsearch is doing.

## How is tracing implemented?

The Elasticsearch server code contains a [tracing][tracing] package, which is
an abstraction over the OpenTelemetry API. All locations in the code that
perform instrumentation and tracing must use these abstractions.

Separately, the [apm](./modules/apm) module implements this interface using the
OpenTelemetry API. When an OTLP endpoint is configured, the module uses its own
OpenTelemetry SDK and exports traces over OTLP/gRPC. Existing installations
without an OTLP endpoint continue to use Elastic's [APM agent for Java][agent]
when configured with an agent server URL. The SDK path does not require the
agent for tracing; the agent can still be used independently for metrics.

## How is tracing configured?

To export traces via OTLP/gRPC, configure an endpoint and enable tracing in
`elasticsearch.yml`:

```
telemetry.tracing.enabled: true
telemetry.export.endpoint: https://<your-otlp-receiver>:4317
```

The endpoint is a host and port without a path. For authentication, add an API
key under `telemetry.api_key` or a secret token under `telemetry.secret_token`
in the Elasticsearch keystore. For example:

    bin/elasticsearch-keystore add telemetry.api_key

The OTLP exporter uses these credentials in an Authorization header. Do not put
credentials in the endpoint URL or JVM options.

When `telemetry.export.endpoint` is present, SDK tracing is selected
automatically. Set `-Dtelemetry.otel.traces.enabled=false` in `config/jvm.options`
to retain agent tracing during migration, or set it to `true` to require the SDK
path even without an endpoint (in which case trace export remains disabled until
an endpoint is supplied). This switch requires a restart. The setting
`telemetry.tracing.enabled` can still be changed dynamically.

For the SDK path, use `telemetry.tracing.sample_rate` (default `0.001`),
`telemetry.tracing.max_depth` (default `0`, exporting only entry-point spans),
`telemetry.tracing.max_queue_size`, `telemetry.tracing.max_batch_size`,
`telemetry.tracing.record_exception_stacks`, and `telemetry.export.interval`.
The SDK batches spans with a bounded queue and flushes on shutdown. Sampling,
batch size, endpoint, and export interval are node settings that require a
restart; trace enablement, maximum depth, name filters, and exception-stack
recording are dynamic.

Existing `telemetry.agent.server_url` installations continue to use the agent
until an OTLP endpoint is configured. When migrating, map `server_url` to
`telemetry.export.endpoint`, `transaction_sample_rate` to
`telemetry.tracing.sample_rate`, `transaction_max_spans` to
`telemetry.tracing.max_depth`, and `metrics_interval` to
`telemetry.export.interval`. The old agent settings remain available for the
legacy path; they are not all interchangeable with SDK settings. The SDK
defaults to the old sample rate, queue size, and interval when those agent
settings are present. Metrics are selected separately using
`-Dtelemetry.otel.metrics.enabled=true`; an OTLP trace endpoint alone does not
migrate metrics.

### Trace compatibility

HTTP requests are SERVER spans named by the matched REST route; tasks are
spans named by their action. The `es.cluster.name`, `es.node.name`, and task
identifiers remain available as span attributes. The SDK also emits
OpenTelemetry HTTP semantic-convention attributes and resource attributes such
as `service.name`, `service.version`, and `service.instance.id`. Dashboards that
use the agent's intake-specific fields may need to query the corresponding
OTLP attributes. W3C `traceparent` and `tracestate` preserve parentage across
HTTP and transport boundaries; locally created child spans remain filtered by
default. Incoming sampled traces retain parent-based sampling, including
Elastic tracestate information used for representative counts.

Span name inclusion/exclusion and sensitive-field redaction continue to use
`telemetry.tracing.names.include`, `telemetry.tracing.names.exclude`, and
`telemetry.tracing.sanitize_field_names`. Avoid adding request bodies or
sensitive headers as attributes. Exceptions omit stack traces by default on
the SDK path; HTTP 5xx responses set ERROR status, whereas 4xx responses do not.

### Legacy agent configuration

For context, the APM agent pulls configuration from [multiple
sources][agent-config], with a hierarchy that means, for example, that options
set in the config file cannot be overridden via system properties.

For agent-only installations, set `telemetry.agent.server_url` and enable
tracing. Agent settings live under `telemetry.agent` and are propagated to the
agent. Dynamic settings can be changed through the cluster settings REST API.
In order to send tracing data to the APM server, ES needs either a secret token
or an API key. We could configure these in the agent via
system properties, but then their values would be available to any Java code in
Elasticsearch that can read system properties.

Instead, when Elasticsearch bootstraps itself, it compiles all APM settings
together, including any `secret_key` or `api_key` values from the ES keystore,
and writes out a temporary APM config file containing all static configuration
(i.e. values that cannot change after the agent starts).  This file is deleted
as soon as possible after ES starts up. Settings that are not sensitive and can
be changed dynamically are configured via system properties. Calls to the ES
settings REST API are translated into system property writes, which the agent
later picks up and applies.

## Where is tracing data sent?

You need to have an APM server running somewhere. For example, you can create a
deployment in [Elastic Cloud](https://www.elastic.co/cloud/) with Elastic's APM
integration.

## What do we trace?

We primarily trace "tasks". The tasks framework in Elasticsearch allows work to
be scheduled for execution, cancelled, executed in a different thread pool, and
so on. Tracing a task results in a "span", which represents the execution of the
task in the tracing system. We also instrument REST requests, which are not (at
present) modelled by tasks.

A span can be associated with a parent span, which allows all spans in, for
example, a REST request to be grouped together. Spans can track work across
different Elasticsearch nodes.

Elasticsearch also supports distributed tracing via [W3c Trace Context][w3c]
headers. If clients of Elasticsearch send these headers with their requests,
then that data will be forwarded to the APM server in order to yield a trace
across systems.

In rare circumstances, it is possible to avoid tracing a task using
`TaskManager#register(String,String,TaskAwareRequest,boolean)`. For example,
Machine Learning uses tasks to record which models are loaded on each node. Such
tasks are long-lived and are not suitable candidates for APM tracing.

## Thread contexts and nested spans

When a span is started, Elasticsearch tracks information about that span in the
current [thread context][thread-context].  If a new thread context is created,
then the current span information must not be propagated but instead renamed, so
that (1) it doesn't interfere when new trace information is set in the context,
and (2) the previous trace information is available to establish a parent /
child span relationship.  This is done with `ThreadContext#newTraceContext()`.

Sometimes we need to detach new spans from their parent. For example, creating
an index starts some related background tasks, but these shouldn't be associated
with the REST request, otherwise all the background task spans will be
associated with the REST request for as long as Elasticsearch is running.
`ThreadContext` provides the `clearTraceContext`() method for this purpose.

## How to I trace something that isn't a task?

First work out if you can turn it into a task. No, really.

If you can't do that, you'll need to ensure that your class can get access to a
`Tracer` instance (this is available to inject, or you'll need to pass it when
your class is created). Then you need to call the appropriate methods on the
tracer when a span should start and end. You'll also need to manage the creation
of new trace contexts when child spans need to be created.

## What additional attributes should I set?

That's up to you. Be careful not to capture anything that could leak sensitive
or personal information.

## What is "scope" and when should I use it?

Usually you won't need to.

That said, sometimes you may want more details to be captured about a particular
section of code. You can think of "scope" as representing the currently active
tracing context. Using scope allows the APM agent to do the following:

* Enables automatic correlation between the "active span" and logging, where
  logs have also been captured.
* Enables capturing any exceptions thrown when the span is active, and linking
  those exceptions to the span
* Allows the sampling profiler to be used as it allows samples to be linked to
  the active span (if any), so the agent can automatically get extra spans
  without manual instrumentation.

However, a scope must be closed in the same thread in which it was opened, which
cannot be guaranteed when using tasks, making scope largely useless to
Elasticsearch.

In the OpenTelemetry documentation, spans, scope and context are fairly
straightforward to use, since `Scope` is an `AutoCloseable` and so can be
easily created and cleaned up use try-with-resources blocks. Unfortunately,
Elasticsearch is a complex piece of software, and also extremely asynchronous,
so the typical OpenTelemetry examples do not work.

Nonetheless, it is possible to manually use scope where we need more detail by
explicitly opening a scope via the `Tracer`.


[otel]: https://opentelemetry.io/
[thread-context]: ./server/src/main/java/org/elasticsearch/common/util/concurrent/ThreadContext.java
[w3c]: https://www.w3.org/TR/trace-context/
[tracing]: ./server/src/main/java/org/elasticsearch/telemetry
[agent-config]: https://www.elastic.co/guide/en/apm/agent/java/master/configuration.html
[agent]: https://www.elastic.co/guide/en/apm/agent/java/current/index.html
