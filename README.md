# Logstash H3 Filter Plugin

This Logstash filter plugin provides support for creating [Uber H3](https://uber.github.io/h3) 
indexes for a given location field and a given resolution range. 

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/current/contributing-java-plugin.html).

## Configuration

Given the following test event...
```
{
  "my_location" : {
    "lat" : "34.03747"
    "lon" : "-118.41587",
  }
}
```


...and the following configuration...

```
filter {
    h3 {
        source => "my_location"
        target => "h3"
        from => 0
        to => 15
    }
}
```

...the resulting event would look like this:
```
{
  "my_location" : {
    "lat" : "34.03747"
    "lon" : "-118.41587",
  },
  "h3": {
    "0" : "8029fffffffffff",
    "1" : "8129bffffffffff",
    "2" : "8229a7fffffffff",
    "3" : "8329a1fffffffff",
    "4" : "8429a19ffffffff",
    "5" : "8529a19bfffffff",
    "6" : "8629a1987ffffff",
    "7" : "8729a1982ffffff",
    "8" : "8829a199c9fffff",
    "9" : "8929a199c93ffff",
    "10" : "8a29a199c937fff",
    "11" : "8b29a199c932fff",
    "12" : "8c29a199c9327ff",
    "13" : "8d29a199c9326bf",
    "14" : "8e29a199c93268f",
    "15" : "8f29a199c932688"
  }
}
```

## Configuration

| Parameter | Use | Required |
| --- | --- | --- |
|source |The field that contains the locations to index |No (defaults to `location`) |
|target |The field to store the H3 indexes into |No (defaults to `h3`) |
|from |The lowest resolution to generate H3 indexes for |No (defaults to `0`) |
|to |The highest resolution to generate H3 indexes for |No (defaults to `15`) |

## Setup

1. Package the gem

`> ./gradlew gem`

2. Install the plugin

`> bin/logstash-plugin install --no-verify --local /path/to/logstash-filter-h3-1.0.0.gem`

## Bugs & TODO

* Support more H3 [utility methods](https://uber.github.io/h3/#/documentation/api-reference/indexing)
* Support all kinds of ES [`geo_point`](https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html) fields. 
Right now, it only supports location fields expressed as a hash with `lat` and `lon` values.

## Tests

Run the tests with

`> ./gradlew test`