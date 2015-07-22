# Taiji

[IMPORTANT]Moved to https://github.com/crask/kafka-pusher

Taiji is a pusher consumer for kafka.

Taiji can pull message from kafka and push it to more consumer via http call.

## The name

Taiji (Tai chi,太极) is an internal Chinese martial art practiced for both its defense training and its health benefits.

Taiji has some actions such as pull and push,this is similar to this consumer.

## Install

```
go get github.com/crask/kafka-pusher
```

# Run

```
kafka-pusher -c="config.json"
```

```
kafka-pusher -V	#show version
kafka-pusher -t	#test config

    // glog params
    -logtostderr=false
        Logs are written to standard error instead of to files.
    -alsologtostderr=false
        Logs are written to standard error as well as to files.
    -stderrthreshold=ERROR
        Log events at or above this severity are logged to standard
        error as well as to files.
    -log_dir=""
        Log files will be written to this directory instead of the
        default temporary directory.
    -v=0
        Enable V-leveled logging at the specified level.
```

## Config

```
{
    "consumer_groups": [                //support multi callback url
        {
            "worker_num": 16,
            "url": "http://localhost",  //callback url
            "retry_times": 4,           //will retry if the callback request response non 200 code
            "bypass_failed":true,       //auto jump to next if single message were processed failed,set to false if service need to process message exactlly
            "failed_sleep":"12s",       //when bypass_failed set to true,sleep this time before retry
            "timeout": "3s",            //the callback time
            "topics": [                 //topic(s) consumed
                "t1"
            ],
            "zookeepers": [             //zookeeper hosts
                "127.0.0.1:2181"
            ],
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
    ]
}
```
