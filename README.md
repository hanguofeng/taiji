# Taiji

[IMPORTANT]Moved to https://github.com/crask/kafka-pusher

Taiji is a pusher consumer for kafka.

Taiji can pull message from kafka and push it to more consumer via http post call.

## The name

Taiji (Tai chi,太极) is an internal Chinese martial art practiced for both its defense training and its health benefits.

Taiji has some actions such as pull and push,this is similar to this consumer.

## Install

```
go get github.com/crask/kafka-pusher
make
```

# Usage

```
kafka-pusher -c=config.json -log_dir=log -v=2
```

```
    // Kafka pusher params
    -c=config.json
        the config file (default "config.json")
    -V=false
        show version
    -t=false
        test config
    -p=""
        log profile file

    // glog params
    -log_dir=""
        Log files will be written to this directory instead of the
        default temporary directory.
    -v=0
        Enable V-leveled logging at the specified level.
    -vmodule=""
        comma-separated list of pattern=N settings for file-filtered logging
    -logtostderr=false
        Logs are written to standard error instead of to files.
    -alsologtostderr=false
        Logs are written to standard error as well as to files.
    -stderrthreshold=ERROR
        Log events at or above this severity are logged to standard
        error as well as to files.
    -log_backtrace_at=0
        when logging hits line file:N, emit a stack trace (default :0)
```

## Config

```
{
    "consumer_groups": [                        // support multi callback url
        {
            // required parameters(panic if missing)
            "url": "http://localhost",          // callback url, can not be defined twice
            "topics": [                         // topic(s) to consume
                "t1"
            ],
            "zookeepers": [                     // zookeeper hosts
                "127.0.0.1:2181"
            ],

            // optional parameters
            "zk_path": "/chroot",               // zookeeper chroot of callback(default no chroot)
            "worker_num": 16,                   // recommend worker num(judged by arbiter, default is arbiter independent)

            // http transporter specific optional parameters(for backward compatibility purpose)
            "retry_times": 4,                   // will retry if the callback request response non 200 code or net error(default is 0)
            "bypass_failed": true,              // auto jump to next if single message were processed failed, set to false if service need to process message exactly(default false)
            "failed_sleep": "12s",              // when bypass_failed set to false, sleep this time before next bunch of retries(default 1s)
            "timeout": "3s",                    // the callback time(default 1s)
            "serializer": "json",               // serializer for message(json, raw, default is raw, unknown serializer will be treated as json)
            "content_type": "",                 // content_type for message(override message content type
                                                // if no content type is defined in consumed message, application/x-www-form-urlencoded is used
                                                // default is no override)

            // optional parameters added after 2.0.0
            "initial_from_oldest": false,       // if we should start from the oldest record when no positive offset is stored in OffsetStorage(default false)
            "offset": {
                "storage_name": "zookeeper",    // primary offset storage type to fetch/commit(default zookeeper)
                "storage_config": {},           // offset storage independent config(default empty)
                "slave_storage": {              // slave storage config, only commit request is routed(default no slave storage)
                    "file": {}                  // storage_name: storage_config for slave offset storage
                }
            },
            "arbiter_name": "sequential",       // support several types of arbiter, providing capability of sliding window or even more complex message delivery control(default sequential arbiter)
            "arbiter_config": {},               // arbiter independent config(default empty)
            "transporter_name": "http",         // support several types of transporter, make all kinds of backend or batch requests possible(default http)
            "transporter_config": {}            // transporter independent config(default empty)
        },
        {
            "url": "http://localhost/api/b",
            "retry_times": 4,
            "bypass_failed":false,
            "failed_sleep":"2s",
            "timeout": "1s",
            "topics": [
                "m2"
            ],
            "zookeepers": [
                "127.0.0.1:2181"
            ],
            "zk_path": "/chroot"
        }
    ],

    // optional global parameters
    "stat_server_port": 8080,                   // admin server port from stat purpose(8000~10000, default is no admin server)
    "connection_pool_size": 1000                // connection pool size of global http RoundTripper(default is 1000)
}
```
