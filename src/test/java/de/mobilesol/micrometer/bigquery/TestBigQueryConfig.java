package de.mobilesol.micrometer.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.time.Duration;
import java.util.Objects;

public class TestBigQueryConfig implements BigQueryConfig {

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
/*
    @Override
    public boolean skipZeroCounter() {
        // defines if a message is sent to bigquery if the count is 0.
        // The value 'false' is the default micrometer behaviour, but you might want to reduce unnecessary requests.
        return true;
    }

    @Override
    public boolean skipZeroTimer() {
        return true;
    }

    @Override
    public boolean skipZeroHistogram() {
        return true;
    }
*/
    @Override
    public String projectId() {
        return "my-google-project";
    }

    @Override
    public String dataset() {
        return "mydataset";
    }

    @Override
    public String get(String k) {
        return null;
    }

    @Override
    public Duration step() {
        return Duration.ofSeconds(20);
    }

    @Override
    public boolean enabled() {
        // false will deactivate the registry.
        return true;
    }

    @Override
    public boolean sendOnlyOneRequest() {
        return true;
    }

    @Override
    public String measurementExclusionFilter() {
        // define a regular expression to suppress pattern
        return "(http_server_.*|system_.*|process_.*|jvm_.*)";
    }

    @Override
    public String location() {
        return "europe-west3";
    }

    @Override
    public Credentials credentials() {
        try {
            return ServiceAccountCredentials.fromStream(
                    Objects.requireNonNull(this.getClass().getResourceAsStream("/account.json")));
        } catch (Exception e) {
            System.out.println("please place the service account key in src/test/resources/account.json");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}