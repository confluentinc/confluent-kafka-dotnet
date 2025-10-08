### KIP-848 - Migration Guide

#### Overview

- **What changed:**

  The **Group Leader role** (consumer member) is removed. Assignments are calculated by the **Group Coordinator (broker)** and distributed via **heartbeats**.

- **Requirements:**

  - Broker version **4.0.0+**
  - `Confluent.Kafka` version **2.12.0+**: GA (production-ready)

- **Enablement (client-side):**

  - `GroupProtocol=Consumer`
  - `GroupRemoteAssignor=<assignor>` (optional; broker-controlled if `null`; default broker assignor is `uniform`)

#### Available Features

All KIP-848 features are supported including:

- Subscription to one or more topics, including **regular expression (regex) subscriptions**
- Rebalance handlers (**incremental only**)
- Static group membership
- Configurable remote assignor
- Enforced max poll interval
- Upgrade from `classic` protocol or downgrade from `consumer` protocol
- AdminClient changes as per KIP

#### Contract Changes

##### Client Configuration changes

| Classic Protocol (Deprecated Configs in KIP-848) | KIP-848 / Next-Gen Replacement                                                 |
|--------------------------------------------------|--------------------------------------------------------------------------------|
| `PartitionAssignmentStrategy`                    | `GroupRemoteAssignor`                                                          |
| `SessionTimeoutMs`                               | Broker config: `group.consumer.session.timeout.ms` (configurable per group)    |
| `HeartbeatIntervalMs`                            | Broker config: `group.consumer.heartbeat.interval.ms` (configurable per group) |
| `GroupProtocolType`                              | Not used in the new protocol                                                   |

##### Rebalance Handler changes

- The **protocol is fully incremental** in KIP-848.
- ⚠️ The `partitions` list passed to `PartitionsAssignedHandler()` and `PartitionsRevokedHandler()` contains only the **incremental changes** — partitions being **added** or **revoked** — **not the full assignment**, as was the case with `Range` or `RoundRobin` in the classic protocol.
It's similar to the `CooperativeSticky` incremental handlers contract but
number of calls can vary: there isn't a call for each rebalance.
- All assignors under KIP-848 are now **sticky**, including `range`, which was **not sticky** in the classic protocol.

##### Manual partition assignment

- You can still use `Assign()` before being subscribed but after subscribing you can only use `IncrementalAssign()` and `IncrementalUnassign()`.

##### Static Group Membership

- Duplicate `GroupInstanceId` handling:
  - **Newly joining member** is fenced with **UnreleasedInstanceId (fatal)**.
  - (Classic protocol fenced the **existing** member instead.)
- Implications:
  - Ensure only **one active instance per** `GroupInstanceId`.
  - Consumers must shut down cleanly to avoid blocking replacements until session timeout expires.

##### Session Timeout & Fetching

- **Session timeout is broker-controlled**:
  - If the Coordinator is unreachable, a consumer **continues fetching messages** but cannot commit offsets.
  - Consumer is fenced once a heartbeat response is received from the Coordinator.
- In the classic protocol, the client stopped fetching when session timeout expired.

##### Closing / Auto-Commit

- On `Close()` or `Unsubscribe()` with auto-commit enabled:
  - Member retries committing offsets until a timeout expires.
  - Currently uses the **default remote session timeout**.
  - Future **KIP-1092** will allow custom commit timeouts.

##### Error Handling Changes

- `UnknownTopicOrPart` (**subscription case**):
  - No longer returned if a topic is missing in the **local cache** when subscribing; the subscription proceeds.
- `TopicAuthorizationFailed`:
  - Reported once per heartbeat or subscription change, even if only one topic is unauthorized.

##### Summary of Key Differences (Classic vs Next-Gen)

- **Assignment:** Classic protocol calculated by **Group Leader (consumer)**; KIP-848 calculated by **Group Coordinator (broker)**
- **Assignors:** Classic range assignor was **not sticky**; KIP-848 assignors are **sticky**, including range
- **Deprecated configs:** Classic client configs are replaced by `GroupRemoteAssignor` and broker-controlled session/heartbeat configs
- **Static membership fencing:** KIP-848 fences **new member** on duplicate `GroupInstanceId`
- **Session timeout:** Classic enforced on client; KIP-848 enforced on broker
- **Auto-commit on close:** Classic stops at client session timeout; KIP-848 retries until remote timeout
- **Unknown topics:** KIP-848 does not return error on subscription if topic missing
- **Upgrade/Downgrade:** KIP-848 supports upgrade/downgrade from/to `classic` and `consumer` protocols

#### Minimal Example Config

##### Classic Protocol

``` properties
# Optional; default is 'classic'
GroupProtocol=Classic
PartitionAssignmentStrategy=<Range,RoundRobin,CooperativeSticky>
SessionTimeoutMs=45000
HeartbeatIntervalMs=15000
```

##### Next-Gen Protocol / KIP-848

``` properties
GroupProtocol=Consumer

# Optional: select a remote assignor
# Valid options currently: 'uniform' or 'range'
#   GroupRemoteAssignor=<uniform,range>
# If unset, broker chooses the assignor (default: 'uniform')

# Session & heartbeat now controlled by broker:
#   group.consumer.session.timeout.ms
#   group.consumer.heartbeat.interval.ms
```

#### Rebalance Callback Migration

**Note:** The `partitions` list contains **only partitions being added or revoked**, not the full partition list as in the eager protocol.
Ensure this is handled correctly if it updates data related to the assigned partitions.

#### Upgrade and Downgrade

- A group made up entirely of `classic` consumers runs under the classic protocol.
- The group is **upgraded to the consumer protocol** as soon as at least one `consumer` protocol member joins.
- The group is **downgraded back to the classic protocol** if the last `consumer` protocol member leaves while `classic` members remain.
- Both **rolling upgrade** (classic → consumer) and **rolling downgrade** (consumer → classic) are supported.

#### Migration Checklist (Next-Gen Protocol / KIP-848)

1.  Upgrade to **Confluent.Kafka ≥ 2.12.0** (GA release)
2.  Run against **Kafka brokers ≥ 4.0.0**
3.  Set `GroupProtocol=Consumer`
4.  Optionally set `GroupRemoteAssignor`; leave unset for `null` for broker-controlled (default: `uniform`), valid options: `uniform` or `range`
5.  Replace deprecated configs with new ones
6.  Update rebalance callbacks to expect **incremental changes only**
7.  Review static membership handling (`GroupInstanceId`)
8.  Ensure proper shutdown to avoid fencing issues
9.  Adjust error handling for unknown topics and authorization failures
