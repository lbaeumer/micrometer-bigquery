package de.mobilesol.micrometer.bigquery;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class SendMetricTest {

    @BeforeEach
    public void setup() {
        // do not forget to set the environment variable
        // GOOGLE_APPLICATION_CREDENTIALS to your google json key file
    }

    @Test
    public void sendMetric() throws InterruptedException {
        log.info("sending metrics");
        Micrometer.Metrics().counter("justatest", "tag1", "value1").increment();

        Thread.sleep(100000);
    }
}
