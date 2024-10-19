/**
 * 2021 Lui
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.mobilesol.micrometer.bigquery;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.http.HttpTransportOptions;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link StepMeterRegistry} for BigQuery.
 *
 * @author Lui Baeumer
 */
public class BigQueryMeterRegistry extends StepMeterRegistry {

    private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("bigquery-metrics-publisher");
    private static final int MAX_RETRY = 3;
    private final BigQueryConfig config;

    private final Logger logger = LoggerFactory.getLogger(BigQueryMeterRegistry.class);

    private final BigQuery bigquery;

    private final SimpleDateFormat sdf;
    private boolean skipPublishingMetrics;

    /**
     * @param config        Configuration options for the registry that are describable as properties.
     * @param clock         The clock to use for timings.
     * @param threadFactory The thread factory to use to create the publishing thread.
     */
    private BigQueryMeterRegistry(BigQueryConfig config, Clock clock, ThreadFactory threadFactory) {
        super(config, clock);
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        config().namingConvention(new BigQueryNamingConvention());
        this.config = config;
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
                .setProjectId(config.projectId());
        if (config.credentials() != null) {
            builder.setCredentials(config.credentials());
        }

        if (config.proxyHost() != null) {

            // access to www.googleapis.com via proxy
            builder.setTransportOptions(HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> {
                        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(
                                config.proxyHost(), config.proxyPort()));

                        logger.debug("using proxy {}", proxy);
                        return new NetHttpTransport.Builder()
                                .setProxy(proxy).build();
                    })
                    .build());
        }

        if (config.location() != null) {
            logger.debug("set location {}", config.location());
            builder.setLocation(config.location());
        }

        bigquery = builder.build().getService();
        start(threadFactory);
    }

    public static Builder builder(BigQueryConfig config) {
        return new Builder(config);
    }

    @Override
    public void start(ThreadFactory threadFactory) {
        if (config.enabled()) {
            super.start(threadFactory);
            logger.info("Using BigQuery to write metrics");
        }
    }

    @Override
    protected void publish() {
        if (skipPublishingMetrics) {
            return;
        }

        logger.debug("publish to bigquery");

        try {
            for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
                logger.debug("handle batch size {}", batch.size());
                InsertAllRequest.Builder req = InsertAllRequest.newBuilder(config.dataset(), config.table());

                int cnt = 0;
                for (Meter meter : batch) {
                    Stream<Map<String, Object>> mat = meter.match(
                            gauge -> writeGauge(gauge.getId(), gauge.value()),
                            counter -> writeCounter(counter.getId(), counter.count()),
                            this::writeTimer,
                            this::writeSummary,
                            this::writeLongTaskTimer,
                            gauge -> writeGauge(gauge.getId(), gauge.value(getBaseTimeUnit())),
                            counter -> writeCounter(counter.getId(), counter.count()),
                            this::writeFunctionTimer,
                            this::writeMeter);

                    String measurementExclusionFilter = config.measurementExclusionFilter();
                    for (Object metric : mat.toArray()) {
                        Map<String, Object> row = (Map<String, Object>) metric;

                        if (measurementExclusionFilter != null
                                && ((String) row.get("_measurement")).matches(measurementExclusionFilter)) {
                            logger.debug("skip excl row {}", row);
                        } else if (config.skipZeroCounter()
                                && "counter".equals(row.get("metric_type"))
                                && Math.abs((double) row.get("value")) < Double.MIN_VALUE) {
                            logger.debug("skip zero counter row {}", row);
                        } else if (config.skipZeroTimer()
                                && "timer".equals(row.get("metric_type"))
                                && (row.get("count") == null || ((long) row.get("count")) == 0)) {
                            logger.debug("skip zero timer row {}", row);
                        } else if (config.skipZeroHistogram()
                                && "histogram".equals(row.get("metric_type"))
                                && (row.get("count") == null || Math.abs((double) row.get("count")) < Double.MIN_VALUE)) {
                            logger.debug("skip zero histogram row {}", row);
                        } else {
                            cnt++;
                            logger.debug("add row={}", row);
                            req.addRow(row);
                        }
                    }
                }

                if (cnt > 0) {
                    logger.debug("preparing to publish {} entries.", cnt);
                    InsertAllRequest request = req.build();

                    // send to bigquery
                    int i = 0;
                    while (i <= MAX_RETRY) {
                        logger.debug("publish {} entries. attempt {}/" + MAX_RETRY, cnt, i);
                        InsertAllResponse resp;
                        try {
                            resp = bigquery.insertAll(request);
                        } catch (BigQueryException e) {
                            logger.warn("could not send metrics to BigQuery, error={}", e.getCode(), e);
                            if (e.getCode() == 404) {
                                if (config.autoCreateDataset()) {
                                    logger.info("BigQuery table {} was not found, so try to create it.", config.table());
                                    createDatasetAndTable(request);
                                    // give bq some seconds to create the missing structures before trying again.
                                    Thread.sleep(5000);
                                } else {
                                    logger.warn("dataset was not found, it will not be created for you. " +
                                            "You might want to set autoCreateDataset to let Micrometer create the dataset for you." +
                                            " Alternatively you have to create the dataset manually.");
                                    skipPublishingMetrics = true;
                                }
                            } else if (e.getCode() == 403) {
                                logger.warn("You do not have enough privileges to write to BigQuery. Please check your service account {}", config.credentials());
                                skipPublishingMetrics = true;
                            }
                            i++;
                            continue;
                        }
                        logger.debug("insert operation result {}", resp);

                        if (resp.hasErrors()) {
                            logger.error("sending to BigQuery failed with {}; attempt {}/" + MAX_RETRY, resp, i);
                            if (i < MAX_RETRY && config.autoCreateFields()) {
                                createNewFields(request, resp);

                                // it might take some seconds for the changes to become into effect
                                logger.debug("try again ... {}/" + MAX_RETRY, i);
                                i++;
                                Thread.sleep(5000L * i * i);
                            } else {
                                break;
                            }
                        } else {
                            logger.debug("successfully sent {}/{} items to BigQuery", request.getRows().size(), batch.size());
                            break;
                        }
                    }
                } else {
                    logger.debug("skipped sending {} items to BigQuery", batch.size());
                }
            }
        } catch (BigQueryException e) {
            logger.warn("could not send metrics to BigQuery, error={}", e.getCode(), e);
        } catch (Throwable e) {
            logger.error("failed to send metrics to BigQuery", e);
        }
    }

    private void createDatasetAndTable(InsertAllRequest request) {
        try {
            Table table = bigquery.getTable(config.dataset(), config.table());
            if (table == null) {

                Dataset dataset = bigquery.getDataset(config.dataset());
                // create the dataset if it does not exist
                logger.debug("the table {} does not exist. dataset={}", config.table(), dataset);
                if (dataset == null) {
                    DatasetInfo.Builder datasetInfoBuilder = DatasetInfo.newBuilder(config.dataset());
                    if (config.location() != null) {
                        datasetInfoBuilder.setLocation(config.location());
                    }

                    bigquery.create(datasetInfoBuilder.build());
                    logger.info("successfully triggered creating BigQuery dataset {}.", config.dataset());
                }

                // create the table
                List<com.google.cloud.bigquery.Field> minimumSchema = new ArrayList<>();
                minimumSchema.add(com.google.cloud.bigquery.Field.of("_time", StandardSQLTypeName.TIMESTAMP));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("_measurement", StandardSQLTypeName.STRING));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("metric_type", StandardSQLTypeName.STRING));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("value", StandardSQLTypeName.BIGNUMERIC));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("sum", StandardSQLTypeName.BIGNUMERIC));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("mean", StandardSQLTypeName.BIGNUMERIC));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("upper", StandardSQLTypeName.BIGNUMERIC));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("count", StandardSQLTypeName.BIGNUMERIC));
                minimumSchema.add(com.google.cloud.bigquery.Field.of("duration", StandardSQLTypeName.BIGNUMERIC));

                // adding missing fields from request
                Map<String, StandardSQLTypeName> missingFields = new TreeMap<>();
                for (InsertAllRequest.RowToInsert row : request.getRows()) {
                    determinePotentiallyNonExistingFields(row, missingFields);
                }
                for (com.google.cloud.bigquery.Field f : minimumSchema) {
                    missingFields.remove(f.getName());
                }
                logger.debug("adding fields={}", missingFields);
                for (String newField : missingFields.keySet()) {
                    minimumSchema.add(com.google.cloud.bigquery.Field.of(newField, missingFields.get(newField)));
                }

                Schema schema = Schema.of(minimumSchema);

                TableId tableId = TableId.of(config.dataset(), config.table());
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

                bigquery.create(tableInfo);
                logger.info("successfully triggered creating BigQuery table {}.", config.table());
            } else {
                logger.debug("table {} exists. nothing to do.", config.table());
            }
        } catch (Throwable e) {
            logger.error("unable to create BigQuery dataset {}", config.dataset(), e);
        }
    }

    private void determinePotentiallyNonExistingFields(InsertAllRequest.RowToInsert row,
                                                       Map<String, StandardSQLTypeName>
                                                               potentiallyNonExistingFields) {
        row.getContent().forEach((key, value) -> {

            if (value instanceof Integer || value instanceof Long) {
                potentiallyNonExistingFields.put(key, StandardSQLTypeName.INT64);
            } else if (value instanceof BigDecimal || value instanceof Double) {
                potentiallyNonExistingFields.put(key, StandardSQLTypeName.BIGNUMERIC);
            } else if (value instanceof Number) {
                potentiallyNonExistingFields.put(key, StandardSQLTypeName.NUMERIC);
            } else if (value instanceof String) {
                potentiallyNonExistingFields.put(key, StandardSQLTypeName.STRING);
            } else {
                logger.warn("BigQuery mapping unknown type {}. map to string", value.getClass());
                potentiallyNonExistingFields.put(key, StandardSQLTypeName.STRING);
            }
        });
    }

    // create new fields in BigQuery if these are missing
    private void createNewFields(InsertAllRequest request, InsertAllResponse resp) {
        Map<String, StandardSQLTypeName>
                potentiallyNonExistingFields = new TreeMap<>();

        resp.getInsertErrors().forEach((i, e) -> e.forEach(error -> {
            // if fields are missing these have to be created
            if (error.getMessage().startsWith("no such field")) {
                InsertAllRequest.RowToInsert row = request.getRows().get(i.intValue());
                determinePotentiallyNonExistingFields(row, potentiallyNonExistingFields);
            } else {
                logger.debug("unknown field inserting error: {}", error);
            }
        }));

        if (!potentiallyNonExistingFields.isEmpty()) {
            logger.debug("potentiallyNonExisting={}", potentiallyNonExistingFields);
            Table table = bigquery.getTable(config.dataset(), config.table());
            Schema newSchema = determineNewSchema(table, potentiallyNonExistingFields);

            if (newSchema != null) {
                // Update the table with the new schema
                logger.debug("now update table schema");
                Table updatedTable =
                        table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
                updatedTable.update();

                logger.debug("successfully updated fields {}", newSchema.getFields());
            }
        }
    }

    private Schema determineNewSchema(Table table, Map<String, StandardSQLTypeName> potentiallyNonExistingFields) {

        // Create the new field/column
        List<com.google.cloud.bigquery.Field> fieldList = new ArrayList<>(
                table.getDefinition() != null
                        ? table.getDefinition().getSchema().getFields()
                        : Collections.emptyList());
        Set<String> existingFields = new HashSet<>();
        fieldList.forEach(f -> existingFields.add(f.getName()));
        logger.debug("existingFields={}", existingFields);

        Map<String, StandardSQLTypeName> createdFields = new HashMap<>();
        potentiallyNonExistingFields.forEach((n, t) -> {
            if (!existingFields.contains(n)) {
                createdFields.put(n, t);
                fieldList.add(com.google.cloud.bigquery.Field.of(n, t));
            }
        });

        if (createdFields.isEmpty()) {
            return null;
        } else {
            logger.info("going to create new BigQuery fields {}", createdFields);
            return Schema.of(fieldList);
        }
    }

    // VisibleForTesting
    Stream<Map<String, Object>> writeMeter(Meter m) {
        List<Field> fields = new ArrayList<>();
        for (Measurement measurement : m.measure()) {
            double value = measurement.getValue();
            if (!Double.isFinite(value)) {
                continue;
            }
            String fieldKey = measurement.getStatistic().getTagValueRepresentation()
                    .replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
            fields.add(new Field(fieldKey, value));
        }
        if (fields.isEmpty()) {
            return Stream.empty();
        }
        Meter.Id id = m.getId();
        return Stream.of(bigqueryParams(id, id.getType().name().toLowerCase(), fields.stream()));
    }

    private Stream<Map<String, Object>> writeLongTaskTimer(LongTaskTimer timer) {
        Stream<Field> fields = Stream.of(
                new Field("active_tasks", timer.activeTasks()),
                new Field("duration", timer.duration(getBaseTimeUnit()))
        );
        return Stream.of(bigqueryParams(timer.getId(), "long_task_timer", fields));
    }

    // VisibleForTesting
    Stream<Map<String, Object>> writeCounter(Meter.Id id, double count) {
        if (Double.isFinite(count)) {
            return Stream.of(bigqueryParams(id, "counter", Stream.of(new Field("value", count))));
        }
        return Stream.empty();
    }

    // VisibleForTesting
    Stream<Map<String, Object>> writeGauge(Meter.Id id, Double value) {
        if (Double.isFinite(value)) {
            return Stream.of(bigqueryParams(id, "gauge", Stream.of(new Field("value", value))));
        }
        return Stream.empty();
    }

    // VisibleForTesting
    Stream<Map<String, Object>> writeFunctionTimer(FunctionTimer timer) {
        double sum = timer.totalTime(getBaseTimeUnit());
        if (Double.isFinite(sum)) {
            Stream.Builder<Field> builder = Stream.builder();
            builder.add(new Field("sum", sum));
            builder.add(new Field("count", timer.count()));
            double mean = timer.mean(getBaseTimeUnit());
            if (Double.isFinite(mean)) {
                builder.add(new Field("mean", mean));
            }
            return Stream.of(bigqueryParams(timer.getId(), "histogram", builder.build()));
        }
        return Stream.empty();
    }

    private Stream<Map<String, Object>> writeTimer(Timer timer) {
        final Stream<Field> fields = Stream.of(
                new Field("sum", timer.totalTime(getBaseTimeUnit())),
                new Field("count", timer.count()),
                new Field("mean", timer.mean(getBaseTimeUnit())),
                new Field("upper", timer.max(getBaseTimeUnit()))
        );

        return Stream.of(bigqueryParams(timer.getId(), "histogram", fields));
    }

    private Stream<Map<String, Object>> writeSummary(DistributionSummary summary) {
        final Stream<Field> fields = Stream.of(
                new Field("sum", summary.totalAmount()),
                new Field("count", summary.count()),
                new Field("mean", summary.mean()),
                new Field("upper", summary.max())
        );

        return Stream.of(bigqueryParams(summary.getId(), "histogram", fields));
    }

    private Map<String, Object> bigqueryParams(Meter.Id id, String metricType, Stream<Field> fields) {
        Map<String, Object> m = getConventionTags(id).stream()
                .filter(t -> StringUtils.isNotBlank(t.getValue()))
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
        m.put("_measurement", getConventionName(id));
        m.put("_time", sdf.format(clock.wallTime()));
        m.put("metric_type", metricType);

        m.putAll(fields.collect(Collectors.toMap(Field::getKey, Field::getValue)));

        return m;
    }

    @Override
    protected final TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    public static class Builder {
        private final BigQueryConfig config;
        private Clock clock = Clock.SYSTEM;
        private ThreadFactory threadFactory = DEFAULT_THREAD_FACTORY;

        Builder(BigQueryConfig config) {
            this.config = config;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public BigQueryMeterRegistry build() {
            return new BigQueryMeterRegistry(config, clock, threadFactory);
        }
    }

    public static class Field {

        private final String key;
        private final double value;

        Field(String key, double value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public double getValue() {
            return value;
        }
    }
}
