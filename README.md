# micrometer-bigquery

This is a Google Cloud BigQuery registry implementation for [micrometer.io](https://github.com/micrometer-metrics/micrometer).
For getting an overview to the micrometer facade, please refer to micrometer.io.
Micrometer provides several registry implementations including Google Stackdriver. BigQuery is the Google Datawarehouse implementation and
provides interesting opportunities to visualize the users activity.

## Initial Setup

1. adding dependency to your pom.xml

```xml
<dependency>
    <groupId>de.mobilesol.micrometer</groupId>
    <artifactId>micrometer-bigquery</artifactId>
    <version>1.0.0</version>
</dependency>
```

2. Initializing micrometer in your code. This is an example taken from a productive project.
```java
import de.mobilesol.micrometer.bigquery.BigQueryConfig;
import de.mobilesol.micrometer.bigquery.BigQueryMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

import java.time.Duration;

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
            public boolean sendOnlyOneRequest() {
                // if you want to send a meter only once to BigQuery
                return true;
            }

            @Override
            public String projectId() {
                return "my-google-project-id";
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
                // define a regular expression to suppress measurement pattern
                return "(http_server_.*|system_.*|process_.*|jvm_.*)";
            }

            @Override
            public String location() {
                return "europe-west3";
            }
        };

        MeterRegistry registry = BigQueryMeterRegistry.builder(config)
                .clock(Clock.SYSTEM)
                .build();
        Metrics.addRegistry(registry);

        return Metrics.globalRegistry;
    }
}

```
This code will run on Google AppEngine without additional authentication.

3. Sending metrics in your code. This is equal to the standard micrometer behaviour.
```java
Micrometer.Metrics().counter("do.something.count",
    "tag1", "somevalue1",
    "tag2", "somevalue2").increment();
```

## Visualizing the result

BigQuery provides some interesting possibilities to visualize the output.
One example from a productive service is shown below. Checkout
[Looker Studio](https://cloud.google.com/looker-studio) for more details.

![Example Looker Report](https://quaestio24.de/images/looker.jpg)

There are more fancy tools available, e.g. Grafana would be interesting.

