Usage:
```shell
gradle run --args="path_to_log_file"
```

Sample input:
```json
{"id":"b3e5f465-1818-4453-b747-37ffb11138dc","state":"STARTED","type":"9cc6be74-3442-4f51-966d-143550300f2d","host":"9d99220b-1070-4308-94ad-519ad27f174f","timestamp":1612136959389}
```

Perf test scenario:
* input data -> https://mega.nz/file/jNxGyCra#2YwEMuJnxz6X7AJZTvd_cmJzBAP4hRYIpCjx8H3q8G0
* 20000000 balanced entries in an input file
* 10000000 rows written to HSQLDB in 2m 33s

TODOs:
* apply producer - consumer to fill and drain event queue asynchronously
* test coverage!