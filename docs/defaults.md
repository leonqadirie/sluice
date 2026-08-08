# Defaults and timeouts

Every option has a default, chosen for pipelines with cheap events. The
table lists them all; the section below explains what happens when a
subscription runs out of time.

## Defaults

| Option              | Where               | Default     |
| ------------------- | ------------------- | ----------- |
| `max_demand`        | per subscription    | 1000        |
| `min_demand`        | per subscription    | 750         |
| `cancel_mode`       | per subscription    | `Permanent` |
| `subscribe_timeout` | per subscription    | 5000 ms     |
| `buffer_capacity`   | source builder      | 10,000      |
| `buffer_capacity`   | gate builder        | no limit    |
| `buffer_keep`       | source and gate     | `KeepLast`  |
| `start_timeout`     | stage builders      | 5000 ms     |
| `on_discard`        | source and gate     | log warning |
| child restart       | consumer supervisor | `Temporary` |
| restart tolerance   | consumer supervisor | 3 in 5 s    |
| `shutdown_timeout`  | consumer supervisor | 5000 ms     |

## Subscription timeouts

`subscribe` waits for the consumer stage for up to `subscribe_timeout`.
If time runs out, it returns `Error(SubscribeTimeout)` and withdraws the
request. The request can't become a live subscription later: sluice does
not send its subscription or initial demand to the producer, and does not
call the consumer's `on_subscribed` or `on_cancelled` callbacks for it.
It's safe to retry the subscription.

A successful `subscribe` means the consumer sent the subscription to the
producer. The producer's acceptance is still asynchronous — for example,
a dispatcher's refusal arrives later as an abnormal end of the
subscription.
