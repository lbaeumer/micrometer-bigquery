/**
 * Copyright 2021 VMware, Inc.
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

import io.micrometer.common.lang.Nullable;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;

import java.util.regex.Pattern;

/**
 * {@link NamingConvention} for BigQuery.
 *
 * @author Lui Baeumer
 */
public class BigQueryNamingConvention implements NamingConvention {

    private static final Pattern PATTERN_SPECIAL_CHARACTERS = Pattern.compile("([, =\"])");

    private final NamingConvention nameDelegate;

    /**
     * By default, BigQuery's configuration option for {@code metric_separator}
     * is an underscore, which corresponds to {@link NamingConvention#snakeCase}.
     */
    public BigQueryNamingConvention() {
        this(NamingConvention.snakeCase);
    }

    public BigQueryNamingConvention(NamingConvention nameDelegate) {
        this.nameDelegate = nameDelegate;
    }

    @Override
    public String name(String name, Meter.Type type, @Nullable String baseUnit) {
        return sanitize(nameDelegate.name(name, type, baseUnit).replace("=", "_"));
    }

    @Override
    public String tagKey(String key) {
        // `time` cannot be a field key or tag key
        if (key.equals("_TABLE_") || key.equals("_FILE_") || key.equals("_PARTITION")) {
            throw new IllegalArgumentException(key + " is an invalid tag key in BigQuery");
        }
        return sanitize(nameDelegate.tagKey(key));
    }

    @Override
    public String tagValue(String value) {
        return sanitize(this.nameDelegate.tagValue(value).replace('\n', ' '));
    }

    private String sanitize(String string) {
        return PATTERN_SPECIAL_CHARACTERS.matcher(string).replaceAll("\\\\$1");
    }
}
