use anyhow::Result;
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace as sdktrace};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::ObservabilityConfig;

pub fn init_tracing(
    service_name: &str,
    default_filter: &str,
    observability: &ObservabilityConfig,
) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let fmt_layer = if std::env::var("SENTINELMESH_LOG_FORMAT").as_deref() == Ok("json") {
        fmt::layer().json().flatten_event(true).boxed()
    } else {
        fmt::layer().compact().boxed()
    };

    if let Some(otlp) = &observability.otlp {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(otlp.endpoint.clone())
            .build()?;
        let provider = sdktrace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                Resource::builder_empty()
                    .with_attributes([
                        KeyValue::new("service.name", otlp.service_name.clone()),
                        KeyValue::new("deployment.environment", otlp.environment.clone()),
                    ])
                    .build(),
            )
            .build();
        let tracer = provider.tracer(service_name.to_owned());
        global::set_tracer_provider(provider);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()?;
    }

    Ok(())
}
