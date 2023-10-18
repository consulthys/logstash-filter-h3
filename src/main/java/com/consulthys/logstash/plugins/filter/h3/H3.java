package com.consulthys.logstash.plugins.filter.h3;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import com.uber.h3core.H3Core;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@LogstashPlugin(name = "h3")
public class H3 implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "location");
    public static final PluginConfigSpec<String> TARGET_CONFIG =
            PluginConfigSpec.stringSetting("target", "h3");
    public static final PluginConfigSpec<Long> FROM_CONFIG =
            PluginConfigSpec.numSetting("from", 0);
    public static final PluginConfigSpec<Long> TO_CONFIG =
            PluginConfigSpec.numSetting("to", 15);

    private String id;
    private String sourceField;
    private String targetField;
    private int from;
    private int to;
    private final H3Core h3;


    public H3(String id, Configuration config, Context context) {
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.targetField = config.get(TARGET_CONFIG);
        this.from = config.get(FROM_CONFIG).intValue();
        this.to = config.get(TO_CONFIG).intValue();

        if (to < from) {
            throw new IllegalArgumentException("'from' must be smaller than 'to'");
        }
        if (from < 0 || from > 15) {
            throw new IllegalArgumentException("'from' must be between 0 and 15, but was " + from);
        }
        if (to < 0 || to > 15) {
            throw new IllegalArgumentException("'to' must be between 0 and 15, but was " + to);
        }

        try {
            this.h3 = AccessController.doPrivileged(
                    (PrivilegedExceptionAction<H3Core>) (() -> H3Core.newInstance())
            );
        } catch (PrivilegedActionException pae) {
            throw new IllegalArgumentException("Could not instantiate H3 engine");
        }
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        for (Event e : events) {
            Object source = e.getField(this.sourceField);
            if (source instanceof Map) {
                Map location = (Map) source;
                Double lat = Double.parseDouble(location.get("lat").toString());
                Double lon = Double.parseDouble(location.get("lon").toString());

                Map<String, String> h3Indexes = new TreeMap<>();
                for (int res = this.from; res <= this.to; res++) {
                    h3Indexes.put(String.valueOf(res), h3.latLngToCellAddress(lat, lon, res));
                }

                e.setField(this.targetField, h3Indexes);
                matchListener.filterMatched(e);
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(SOURCE_CONFIG, TARGET_CONFIG, FROM_CONFIG, TO_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
