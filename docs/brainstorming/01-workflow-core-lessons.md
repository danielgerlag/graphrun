# What to keep and change from Workflow Core

This source investigation is background material. Current implementation requirements are in [the v1 specifications](../specs/v1/README.md).

Workflow Core already treats queued IDs as prompts to inspect persisted state. The queue is not the complete execution record.

The strongest improvement is to make persisted progress and scheduling one atomic operation. Replacing a polling timer while retaining separate persistence, queue, and lock contracts leaves most of the difficult failure cases intact.

This critique uses commit `f64d503a7217b1f314a23379da1f4c6bec70c267`. Findings describe source behavior and its architectural consequences, not reproduced production incidents.

## How the current engine works

A registered workflow becomes a graph of `WorkflowStep` objects. A running instance records the definition ID, version, shared data, and a collection of execution pointers.

Starting a workflow persists the instance, sends workflow and index queue items, and publishes a lifecycle notification as separate calls. Publishing an application event similarly persists the event before queueing its ID. [W01]

A workflow consumer acquires a workflow lock, loads the instance, and calls the executor. The executor walks eligible pointers and awaits their steps. Inputs come from workflow data and outputs are assigned back to it. The consumer then persists the execution round. [W02] [W03] [W21]

The executor updates pointers to represent progress, sleeps, waits, and branches. It derives the next execution time from those pointers and their child scopes. Near-term wakeups can use an in-process delay. Later wakeups can use scheduled commands when the provider supports them. [W02] [W04]

`RunnablePoller` periodically inspects persisted runnable workflows, events, and scheduled commands. That mechanism supplies normal delayed scheduling and recovery from gaps between persistence and queue operations. [W05]

The queue consumer tracks active IDs locally. `GreyList` also suppresses some repeated enqueues locally. Distributed locks constrain concurrent processing while lock ownership remains valid. These mechanisms reduce redundant work but do not make queue insertion globally unique. [W06] [W07]

```text
workflow admission -> persist instance -> queue workflow ID
                                             |
                                             v
                                     acquire workflow lock
                                             |
                                             v
                                   load and execute pointers
                                             |
                                             v
                              persist progress and subscriptions
                                             |
                                             v
                                   arrange the next wakeup

periodic persistence scan ----------------> queue workflow ID
```

## Evidence-backed architectural findings

| Finding | Current behavior and qualification | Change worth making |
|---|---|---|
| Persistence and queueing have separate failure boundaries | Start and subsequent wake scheduling use separate calls. Durable state often lets the poller recover the gap. [W01] [W02] | Commit readiness with progress in one backend transaction |
| Polling is part of the scheduling architecture | A recurring timer invokes workflow, event, and command polling. Removing it alone removes recovery behavior too. [W05] | Wake from committed changes and known deadlines, with an explicit recovery protocol |
| Duplicate suppression is local or provider-specific | Active-ID tracking, second passes, and greylisting do not form a durable uniqueness constraint. Some second passes intentionally handle work arriving during execution. [W06] [W07] | Deduplicate logical activations, while retaining newly accepted input for another state transition |
| The queue contract cannot express acknowledgement ownership | `DequeueWork` returns a string, not a delivery receipt. The SQS provider deletes a received message before returning it to the consumer. [W08] [W09] | Avoid a separate broker initially. If one is added, specify acknowledgement and recovery semantics explicitly |
| Lock ownership is absent from the persistence contract | The lock interface returns acquisition success and releases by key. The EF save path has no lock-owner token parameter or stale-owner predicate. [W10] [W11] | Require conditional state transitions with ownership tokens because clustered workers can claim the same work |
| User activity duration extends workflow lock duration | The consumer holds its workflow lock across the executor's awaited steps. The executor visits eligible pointers sequentially in this path. [W02] [W03] | Separate short durable claims from user I/O, and commit isolated activity results |
| Execution state has overlapping representations | `ExecutionPointer` combines status, `Active`, timestamps, event flags, and object-valued data. `ExecutionResult` independently exposes proceed, sleep, event, and branch fields. [W12] [W13] | Model mutually exclusive states and outcomes as enums with required payloads |
| Durable step identity depends on compiled graph layout | The builder assigns integer IDs from list length and rewrites IDs while attaching branches. External names and workflow versions exist, but persisted pointers use `StepId`. [W14] [W12] | Use stable node keys within immutable, validated definition versions |
| Storage payloads can depend on runtime type names | The EF serializer uses `TypeNameHandling.All` for workflow and pointer payloads. This is provider-specific, not a claim about every store. [W15] | Use explicit schema names, versions, and codecs rather than CLR or Rust type names |
| Instance persistence traverses accumulated pointer state | The EF path loads pointers and serializes their payloads. Next-execution calculation also visits pointer state. This establishes history-sensitive work, not a measured bottleneck or a claim that every database row is rewritten. [W11] [W15] [W04] | Update affected activations and indexed scheduling state, with explicit history retention |
| Some scheduling failures lack a durable explanation | EF command scheduling catches `DbUpdateException`, and command processing catches `Exception`, without surfacing either from those catch blocks. [W16] | Distinguish expected duplicate conflicts from storage failures and expose blocked or unhealthy runtime state |
| Event consumption spans multiple persistence operations | Event delivery updates workflow pointers, terminates a subscription, and queues work through separate operations. Existing pointer checks and event reseeding mitigate repetition. [W17] [W02] | Persist inbox consumption, wait resolution, and readiness atomically |

These are not all equally urgent. Atomic transitions, durable identity, and ownership semantics determine correctness. Reflection and builder ergonomics matter, but changing syntax first would leave the difficult parts untouched.

## Concrete failure windows

### A run commits before its queue item exists

`StartWorkflow` persists a runnable instance before calling `QueueWork`. A process failure between those calls leaves a durable run without its immediate wakeup. [W01]

The poller can rediscover that run. This is evidence that polling repairs a split operation, not evidence that every such failure loses a workflow.

### A command queues work before being deleted

The EF scheduled-command processor invokes its supplied action, then removes the command through another context. [W16]

If the queue action succeeds and the process stops before deletion, the same command can be processed again. Queue-provider deduplication may suppress the redundant send, but the command protocol itself does not make the action and deletion atomic.

### A queue message disappears before execution

The SQS provider receives a message, deletes it, and returns its body. [W09]

A worker failure after that deletion can remove the immediate delivery path. Durable runnable state can still permit recovery through polling. This behavior must not be described as acknowledgement after successful processing.

### An effect succeeds before the execution round is saved

The executor awaits user steps and mutates in-memory pointer state. The consumer persists after the executor returns. [W02] [W03]

A process failure after a successful external call but before persistence can cause recovery to retry work whose external effect already happened. This is a general workflow problem. Rust, queue uniqueness, and local database transactions cannot solve it without cooperation from the external system.

The reimagined engine should expose stable effect keys and ambiguous outcomes directly. See the [failure semantics](03-execution-and-failure-semantics.md).

## Important corrections to an overly broad critique

Workflow Core does have versioned definitions. The executor looks up the instance's exact definition version. The weakness is the compatibility and deployment contract around those definitions, not an absence of versioning. [W03] [W18]

Workflow Core also has an early-event recovery path. After subscriptions are persisted, `TryProcessSubscription` searches prior matching events using `SubscribeAsOf`, marks them unprocessed, and queues them again. A new design must preserve the intended capability rather than replace it with transient notifications. [W02]

The EF provider can save a workflow and newly created subscriptions in one `SaveChangesAsync` operation. It would be inaccurate to say that every persistence update is independent. Queueing and the later event-consumption sequence still cross separate operations. [W11] [W17]

Logical parallel branches do not imply concurrent mutation by multiple threads inside the inspected executor loop. That loop awaits each eligible pointer. Separate activity scheduling would be a deliberate new concurrency model. [W03]

Execution-pointer IDs are generated GUIDs. Definition step IDs are integers. These are different identities, and neither should be confused with a stable external effect key. [W19] [W14]

A lock timeout setting alone does not prove that a long-running workflow loses its lock. A provider or its dependency may renew leases. The stronger architectural concern is whether persistence rejects an old owner after ownership is actually lost.

## What is worth preserving

Keep the embedded library model, explicit workflow versions, reusable activities, durable waits, retries, and the ability to inspect workflow progress.

Preserve the distinction between workflow definitions and running instances. Also preserve the concept of handling an event that arrives before a subscription, with a clearer inbox and retention contract.

Compensation is useful, but it needs explicit failure semantics rather than a promise of rollback. Typed authoring is useful too, but a Rust API should enforce more at registration and avoid normal-path downcasts.

Workflow Core's JSON and YAML loader resolves runtime types, constructs step types through reflection, and parses expression strings. That is substantial machinery, not something an embedded Rust engine inherits for free. [W20]

YAML definitions are now a hard requirement for the Rust engine. Preserve the declarative authoring capability, but replace runtime type discovery with an explicit versioned activity catalog. Compile a validated data-only DSL to the same durable graph as any Rust builder.

The architectural improvement is one execution model with stronger definition boundaries, not dropping YAML. The resolved v1 scope excludes arbitrary scripts, expression-string evaluation, and dynamically loaded activity binaries.

## Source map

The links below use the inspected revision. The [source notes](06-sources.md) describe scope and repeatable local inspection.

[W01]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/WorkflowController.cs#L56-L125
[W02]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/BackgroundTasks/WorkflowConsumer.cs#L37-L164
[W03]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/WorkflowExecutor.cs#L42-L109
[W04]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/WorkflowExecutor.cs#L222-L271
[W05]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/BackgroundTasks/RunnablePoller.cs#L35-L180
[W06]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/BackgroundTasks/QueueConsumer.cs#L69-L130
[W07]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/GreyList.cs#L10-L66
[W08]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Interface/IQueueProvider.cs#L11-L32
[W09]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/providers/WorkflowCore.Providers.AWS/Services/SQSQueueProvider.cs#L31-L55
[W10]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Interface/IDistributedLockProvider.cs#L5-L22
[W11]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/providers/WorkflowCore.Persistence.EntityFramework/Services/EntityFrameworkPersistenceProvider.cs#L137-L179
[W12]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Models/ExecutionPointer.cs#L7-L68
[W13]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Models/ExecutionResult.cs#L6-L106
[W14]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/FluentBuilders/WorkflowBuilder.cs#L30-L122
[W15]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/providers/WorkflowCore.Persistence.EntityFramework/ExtensionMethods.cs#L12-L85
[W16]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/providers/WorkflowCore.Persistence.EntityFramework/Services/EntityFrameworkPersistenceProvider.cs#L403-L443
[W17]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/BackgroundTasks/EventConsumer.cs#L97-L154
[W18]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/WorkflowRegistry.cs#L22-L94
[W19]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/ExecutionPointerFactory.cs#L11-L78
[W20]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore.DSL/Services/DefinitionLoader.cs#L100-L170
[W21]: https://github.com/danielgerlag/workflow-core/blob/f64d503a7217b1f314a23379da1f4c6bec70c267/src/WorkflowCore/Services/WorkflowExecutor.cs#L147-L201
