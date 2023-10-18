package com.consulthys.logstash.plugins.filter.h3;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.FilterMatchListener;
import org.junit.Assert;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstash.plugins.ContextImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class H3Test {

    @Test
    public void testH3DefaultConfiguration() {
        // configure the filter
        Configuration config = new ConfigurationImpl(Collections.EMPTY_MAP);
        Context context = new ContextImpl(null, null);
        H3 filter = new H3("test-id", config, context);

        // execute the filter
        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        e.setField("location", location);
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        // assess the resulting event
        Assert.assertEquals(1, matchListener.getMatchCount());
        Assert.assertEquals(1, results.size());
        Map h3 = (Map) e.getField("h3");
        Assert.assertNotNull(h3);
        Assert.assertEquals("8075fffffffffff", h3.get("0"));
        Assert.assertEquals("81757ffffffffff", h3.get("1"));
        Assert.assertEquals("82754ffffffffff", h3.get("2"));
        Assert.assertEquals("83754efffffffff", h3.get("3"));
        Assert.assertEquals("84754a9ffffffff", h3.get("4"));
        Assert.assertEquals("85754e67fffffff", h3.get("5"));
        Assert.assertEquals("86754e64fffffff", h3.get("6"));
        Assert.assertEquals("87754e64dffffff", h3.get("7"));
        Assert.assertEquals("88754e6499fffff", h3.get("8"));
        Assert.assertEquals("89754e64993ffff", h3.get("9"));
        Assert.assertEquals("8a754e64992ffff", h3.get("10"));
        Assert.assertEquals("8b754e649929fff", h3.get("11"));
        Assert.assertEquals("8c754e649929dff", h3.get("12"));
        Assert.assertEquals("8d754e64992d6ff", h3.get("13"));
        Assert.assertEquals("8e754e64992d6df", h3.get("14"));
        Assert.assertEquals("8f754e64992d6d8", h3.get("15"));
        Assert.assertNull(h3.get("16"));
    }

    @Test
    public void testH3SpecificFields() {
        // configure the filter
        Map<String, Object> settings = new HashMap<>();
        settings.put("source", "loc");
        settings.put("target", "indexes");
        Configuration config = new ConfigurationImpl(settings);
        Context context = new ContextImpl(null, null);
        H3 filter = new H3("test-id", config, context);

        // execute the filter
        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        e.setField((String) settings.get("source"), location);
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        // assess the resulting event
        Assert.assertEquals(1, matchListener.getMatchCount());
        Assert.assertEquals(1, results.size());
        Map h3 = (Map) e.getField((String) settings.get("target"));
        Assert.assertNotNull(h3);
        Assert.assertEquals("8075fffffffffff", h3.get("0"));
        Assert.assertEquals("81757ffffffffff", h3.get("1"));
        Assert.assertEquals("82754ffffffffff", h3.get("2"));
        Assert.assertEquals("83754efffffffff", h3.get("3"));
        Assert.assertEquals("84754a9ffffffff", h3.get("4"));
        Assert.assertEquals("85754e67fffffff", h3.get("5"));
        Assert.assertEquals("86754e64fffffff", h3.get("6"));
        Assert.assertEquals("87754e64dffffff", h3.get("7"));
        Assert.assertEquals("88754e6499fffff", h3.get("8"));
        Assert.assertEquals("89754e64993ffff", h3.get("9"));
        Assert.assertEquals("8a754e64992ffff", h3.get("10"));
        Assert.assertEquals("8b754e649929fff", h3.get("11"));
        Assert.assertEquals("8c754e649929dff", h3.get("12"));
        Assert.assertEquals("8d754e64992d6ff", h3.get("13"));
        Assert.assertEquals("8e754e64992d6df", h3.get("14"));
        Assert.assertEquals("8f754e64992d6d8", h3.get("15"));
        Assert.assertNull(h3.get("16"));
    }

    @Test
    public void testH3WithStringLocation() {
        // configure the filter
        Configuration config = new ConfigurationImpl(Collections.EMPTY_MAP);
        Context context = new ContextImpl(null, null);
        H3 filter = new H3("test-id", config, context);

        // execute the filter
        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        Map<String, String> location = new HashMap<>();
        location.put("lat", "0.0");
        location.put("lon", "0.0");
        e.setField("location", location);
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        // assess the resulting event
        Assert.assertEquals(1, matchListener.getMatchCount());
        Assert.assertEquals(1, results.size());
        Map h3 = (Map) e.getField("h3");
        Assert.assertNotNull(h3);
        Assert.assertEquals("8075fffffffffff", h3.get("0"));
        Assert.assertEquals("81757ffffffffff", h3.get("1"));
        Assert.assertEquals("82754ffffffffff", h3.get("2"));
        Assert.assertEquals("83754efffffffff", h3.get("3"));
        Assert.assertEquals("84754a9ffffffff", h3.get("4"));
        Assert.assertEquals("85754e67fffffff", h3.get("5"));
        Assert.assertEquals("86754e64fffffff", h3.get("6"));
        Assert.assertEquals("87754e64dffffff", h3.get("7"));
        Assert.assertEquals("88754e6499fffff", h3.get("8"));
        Assert.assertEquals("89754e64993ffff", h3.get("9"));
        Assert.assertEquals("8a754e64992ffff", h3.get("10"));
        Assert.assertEquals("8b754e649929fff", h3.get("11"));
        Assert.assertEquals("8c754e649929dff", h3.get("12"));
        Assert.assertEquals("8d754e64992d6ff", h3.get("13"));
        Assert.assertEquals("8e754e64992d6df", h3.get("14"));
        Assert.assertEquals("8f754e64992d6d8", h3.get("15"));
        Assert.assertNull(h3.get("16"));
    }

    @Test
    public void testH3SpecificResolution() {
        // configure the filter
        Map<String, Object> settings = new HashMap<>();
        settings.put("from", 8L);
        settings.put("to", 10L);
        Configuration config = new ConfigurationImpl(settings);
        Context context = new ContextImpl(null, null);
        H3 filter = new H3("test-id", config, context);

        // execute the filter
        Event e = new org.logstash.Event();
        TestMatchListener matchListener = new TestMatchListener();
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 0.0);
        location.put("lon", 0.0);
        e.setField("location", location);
        Collection<Event> results = filter.filter(Collections.singletonList(e), matchListener);

        // assess the resulting event
        Assert.assertEquals(1, matchListener.getMatchCount());
        Assert.assertEquals(1, results.size());
        Map h3 = (Map) e.getField("h3");
        Assert.assertNotNull(h3);
        Assert.assertNull(h3.get("0"));
        Assert.assertNull(h3.get("1"));
        Assert.assertNull(h3.get("2"));
        Assert.assertNull(h3.get("3"));
        Assert.assertNull(h3.get("4"));
        Assert.assertNull(h3.get("5"));
        Assert.assertNull(h3.get("6"));
        Assert.assertNull(h3.get("7"));
        Assert.assertEquals("88754e6499fffff", h3.get("8"));
        Assert.assertEquals("89754e64993ffff", h3.get("9"));
        Assert.assertEquals("8a754e64992ffff", h3.get("10"));
        Assert.assertNull(h3.get("11"));
        Assert.assertNull(h3.get("12"));
        Assert.assertNull(h3.get("13"));
        Assert.assertNull(h3.get("14"));
        Assert.assertNull(h3.get("15"));
        Assert.assertNull(h3.get("16"));
    }

    @Test
    public void testH3ComplainsAboutFromBiggerThanTo() {
        // configure the filter
        Map<String, Object> settings = new HashMap<>();
        settings.put("from", 10L);
        settings.put("to", 8L);
        Configuration config = new ConfigurationImpl(settings);
        Context context = new ContextImpl(null, null);

        try {
            H3 filter = new H3("test-id", config, context);
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("'from' must be smaller than 'to'", iae.getMessage());
        }
    }

    @Test
    public void testH3ComplainsAboutFromOutOfRange() throws Exception {
        // configure the filter
        Map<String, Object> settings = new HashMap<>();
        settings.put("from", -1L);
        settings.put("to", 8L);
        Configuration config = new ConfigurationImpl(settings);
        Context context = new ContextImpl(null, null);

        try {
            H3 filter = new H3("test-id", config, context);
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("'from' must be between 0 and 15, but was -1", iae.getMessage());
        }
    }

    @Test
    public void testH3ComplainsAboutToOutOfRange() throws Exception {
        // configure the filter
        Map<String, Object> settings = new HashMap<>();
        settings.put("from", 13L);
        settings.put("to", 17L);
        Configuration config = new ConfigurationImpl(settings);
        Context context = new ContextImpl(null, null);

        try {
            H3 filter = new H3("test-id", config, context);
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("'to' must be between 0 and 15, but was 17", iae.getMessage());
        }
    }
}

class TestMatchListener implements FilterMatchListener {

    private AtomicInteger matchCount = new AtomicInteger(0);

    @Override
    public void filterMatched(Event event) {
        matchCount.incrementAndGet();
    }

    public int getMatchCount() {
        return matchCount.get();
    }
}