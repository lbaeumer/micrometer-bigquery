package de.mobilesol.micrometer.bigquery;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public class Micrometer {

    private static MeterRegistry metrics;

    public static MeterRegistry Metrics() {
        if (metrics == null) {
            metrics = init();
        }
        return metrics;
    }

    private static MeterRegistry init() {
        BigQueryConfig config = new BigQueryConfig() {
            @Override
            public boolean autoCreateDataset() {
                // if you want the dataset to be created automatically
                return true;
            }

            @Override
            public boolean autoCreateFields() {
                // if you want the fields to be created automatically
                return true;
            }

            @Override
            public boolean skipZeroCounter() {
                // defines if a message if sent to bigquery if the count is 0.
                // The value 'false' is the default micrometer behaviour, but you might want to reduce unnecessary requests.
                return true;
            }

            @Override
            public String projectId() {
                return "my-gcp-project";
            }

            @Override
            public String get(String k) {
                return null;
            }

            @Override
            public boolean enabled() {
                // false will deactivate the registry.
                return true;
            }

            @Override
            public String measurementExclusionFilter() {
                // define a regular expression to suppress pattern
                return "(http_server_.*|system_.*|process_.*|jvm_.*)";
            }

            @Override
            public String location() {
                return "EU";
            }
        };

        MeterRegistry registry = BigQueryMeterRegistry.builder(config)
                .clock(Clock.SYSTEM)
                .build();
        Metrics.addRegistry(registry);

        return Metrics.globalRegistry;
    }
}