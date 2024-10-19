package de.mobilesol.micrometer.bigquery;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

@Slf4j
public class SendMetricGaugeTest {

    protected static MeterRegistry metrics;

    @BeforeAll
    public static void setup() {
        // either set the environment variable
        // GOOGLE_APPLICATION_CREDENTIALS to your google json key file
        // or set service account key

        log.info("setting up micrometer");
        BigQueryConfig config = new TestBigQueryConfig() {
            @Override
            public String table() {
                return "gauge";
            }
        };
        MeterRegistry registry = BigQueryMeterRegistry.builder(config)
                .clock(Clock.SYSTEM)
                .build();
        Metrics.addRegistry(registry);
        metrics = Metrics.globalRegistry;
    }

    @AfterAll
    public static void afterAll() throws InterruptedException {
        log.info("waiting before shutting down");
        Thread.sleep(60000);
        log.info("shutting down micrometer");
        Set<MeterRegistry> regs = Metrics.globalRegistry.getRegistries();
        for (MeterRegistry r : regs) {
            Metrics.globalRegistry.remove(r);
        }
        metrics.close();
    }

    @Test
    public void sendGauge1() throws InterruptedException {
        log.info("sending gauge");
        metrics.gauge("mgauge1", 12);
        Thread.sleep(30000);

        metrics.gauge("mgauge1", 18);
        Thread.sleep(30000);

        metrics.gauge("mgauge1", 23);
        Thread.sleep(30000);
    }
}
