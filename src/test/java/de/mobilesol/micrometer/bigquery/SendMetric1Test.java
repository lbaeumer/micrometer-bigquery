package de.mobilesol.micrometer.bigquery;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SendMetric1Test {

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
                return "mytable1";
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
        Thread.sleep(120000);
        log.info("shutting down micrometer");
        Set<MeterRegistry> regs = Metrics.globalRegistry.getRegistries();
        for (MeterRegistry r : regs) {
            Metrics.globalRegistry.remove(r);
        }
        metrics.close();
    }

    @Test
    public void sendCounter1() {
        log.info("sending metrics");
        metrics.counter("mcounter1",
                        "c1", "cval1")
                .increment();
    }

    @Test
    public void sendCounter2() {
        log.info("sending metrics2");
        metrics.counter("mcounter2",
                        "c2", "cval2", "c3", "cval3", "c4", "cval4")
                .increment();
        metrics.counter("mcounter2",
                        "c2", "cval2", "c3", "cval3", "c4", "cval4")
                .increment(2);
    }

    @Test
    public void sendGauge() {
        log.info("sending gauge");
        metrics.gauge("mgauge", 127);
    }

    @Test
    public void sendTimer() {
        log.info("sending timer");
        metrics.timer("mtimer", "t1", "t1val")
                .record(30, TimeUnit.SECONDS);
    }

    @Test
    public void sendSummary() {
        log.info("sending summary");
        metrics.summary("msummary", "s1", "s1val").record(213.123);
    }
}
