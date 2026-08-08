//// The broadcast dispatcher. Each event goes to each subscriber. The flow
//// moves at the speed of the slowest subscriber: events go out only up to
//// the smallest open demand across the subscribers. A subscriber can give
//// a selector function. It then receives only the events that the
//// function keeps. For each filtered event, the dispatcher returns a
//// replacement ask on behalf of that subscriber. The replacement demand
//// goes through the normal ask path and can start new production. Thus
//// the demand accounts of the producer and of the consumer stay equal,
//// and the flow does not stop.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import sluice/internal/dispatcher.{
  type Dispatcher, Delivery, DispatchResult, Dispatcher,
}
import sluice/internal/protocol.{type ConsumerMessage}

type Subscriber(event) {
  Subscriber(
    subject: Subject(ConsumerMessage(event)),
    remaining: Int,
    keep: Option(fn(event) -> Bool),
  )
}

pub fn new() -> Dispatcher(event) {
  make([])
}

fn make(subscribers: List(Subscriber(event))) -> Dispatcher(event) {
  Dispatcher(
    subscribe: fn(from, metadata) {
      // A new subscriber starts with a demand of zero. Thus the flow
      // stops until the new subscriber asks: nobody moves ahead of it.
      let subscriber = Subscriber(from, 0, metadata.selector)
      Ok(make([subscriber, ..subscribers]))
    },
    ask: fn(demand, from) {
      let before = available(subscribers)
      let subscribers =
        list.map(subscribers, fn(subscriber) {
          case subscriber.subject == from {
            True ->
              Subscriber(..subscriber, remaining: subscriber.remaining + demand)
            False -> subscriber
          }
        })
      #(int.max(available(subscribers) - before, 0), [], make(subscribers))
    },
    cancel: fn(from) {
      let before = available(subscribers)
      let subscribers =
        list.filter(subscribers, fn(subscriber) { subscriber.subject != from })
      #(int.max(available(subscribers) - before, 0), make(subscribers))
    },
    dispatch: fn(events, _incoming) {
      let take = int.min(available(subscribers), list.length(events))
      let #(batch, leftover) = list.split(events, take)
      let #(deliveries, reask, subscribers) =
        list.fold(subscribers, #([], [], []), fn(accumulated, subscriber) {
          let #(deliveries, reask, next_subscribers) = accumulated
          let kept = case subscriber.keep {
            None -> batch
            Some(keep) -> list.filter(batch, keep)
          }
          let subscriber =
            Subscriber(..subscriber, remaining: subscriber.remaining - take)
          let deliveries = case kept {
            [] -> deliveries
            _ -> [Delivery(subscriber.subject, kept), ..deliveries]
          }
          // The consumer only counts the received events. A replacement
          // ask for the filtered quantity keeps the two demand accounts
          // equal, and it can start new production.
          let filtered = take - list.length(kept)
          let reask = case filtered > 0 {
            True -> [#(subscriber.subject, filtered), ..reask]
            False -> reask
          }
          #(deliveries, reask, [subscriber, ..next_subscribers])
        })
      DispatchResult(deliveries:, leftover:, reask:, next: make(subscribers))
    },
    total_demand: fn() { available(subscribers) },
  )
}

// The demand that all subscribers can accept together: the smallest open
// demand. Without subscribers there is no demand.
fn available(subscribers: List(Subscriber(event))) -> Int {
  case subscribers {
    [] -> 0
    [first, ..remaining] ->
      list.fold(remaining, first.remaining, fn(smallest, subscriber) {
        int.min(smallest, subscriber.remaining)
      })
  }
}
