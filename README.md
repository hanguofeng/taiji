# Taiji

Taiji is a pusher consumer for kafka.

Taiji can pull message from kafka and push it to more consumer via http call.

## The name

Taiji (Tai chi,太极) is an internal Chinese martial art practiced for both its defense training and its health benefits.

Taiji has some actions such as pull and push,this is similar to this consumer.

## Config

```
{
	  "log_file":"logs/taiji.log",  //the log file,if no this option,will output to stdout
    "callbacks": [                //support multi callback url
        {
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
            "zk_path": "/kafka/cart"    //zookeeper path
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
            "zk_path": "/kafka/cart"
        }
    ]
}
```
