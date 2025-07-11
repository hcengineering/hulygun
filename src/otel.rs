use std::sync::LazyLock;

use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};

use super::config::OtelMode;

static RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
    Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new("service.namespace", "huly"))
        .build()
});

pub fn init() {
    match super::CONFIG.otel {
        OtelMode::On => {
            global::set_meter_provider(
                SdkMeterProvider::builder()
                    .with_periodic_exporter(
                        opentelemetry_otlp::MetricExporter::builder()
                            .with_http()
                            .build()
                            .unwrap(),
                    )
                    .with_resource(RESOURCE.clone())
                    .build(),
            );
        }
        OtelMode::Stdout => {
            global::set_meter_provider(
                SdkMeterProvider::builder()
                    .with_periodic_exporter(
                        opentelemetry_stdout::MetricExporterBuilder::default().build(),
                    )
                    .with_resource(RESOURCE.clone())
                    .build(),
            );
        }
        OtelMode::Off => {
            global::set_meter_provider(
                SdkMeterProvider::builder()
                    .with_resource(RESOURCE.clone())
                    .build(),
            );
        }
    }
}

pub fn tracer_provider() -> Option<SdkTracerProvider> {
    match super::CONFIG.otel {
        OtelMode::On => Some(
            SdkTracerProvider::builder()
                .with_batch_exporter(
                    opentelemetry_otlp::SpanExporter::builder()
                        .with_http()
                        .build()
                        .unwrap(),
                )
                .with_resource(RESOURCE.clone())
                .build(),
        ),
        OtelMode::Stdout => Some(
            SdkTracerProvider::builder()
                .with_batch_exporter(opentelemetry_stdout::SpanExporter::default())
                .with_resource(RESOURCE.clone())
                .build(),
        ),
        OtelMode::Off => None,
    }
}

pub fn logger_provider() -> Option<SdkLoggerProvider> {
    match super::CONFIG.otel {
        OtelMode::On => Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(
                    opentelemetry_otlp::LogExporterBuilder::default()
                        .with_http()
                        .build()
                        .unwrap(),
                )
                .with_resource(RESOURCE.clone())
                .build(),
        ),

        OtelMode::Stdout => Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(opentelemetry_stdout::LogExporter::default())
                .with_resource(RESOURCE.clone())
                .build(),
        ),

        OtelMode::Off => None,
    }
}
