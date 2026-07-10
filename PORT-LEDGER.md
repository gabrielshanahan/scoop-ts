# PORT-LEDGER

Single source of truth for port progress. Statuses: `pending` → `ported` → `verified`.
`verified` for a test means: ported faithfully AND passing against real Postgres.

> **Test-count note:** the port brief cites "219 @Test methods". The reference repo contains
> exactly **195** `@Test` methods; the 219 figure comes from substring-counting `@Test`, which
> also matches the class-level annotations `@TestInstance` (21) and `@TestProfile` (3):
> 195 + 21 + 3 = 219. Those two are JUnit/Quarkus lifecycle annotations, not tests. This ledger
> reconciles against the true total: **195 test methods, none dropped**.

## scoop-core main sources (58 files)

| Kotlin file | TS file | Status |
|---|---|---|
| io/github/gabrielshanahan/scoop/JsonbHelper.kt | src/JsonbHelper.ts | ported |
| io/github/gabrielshanahan/scoop/Scoop.kt | src/Scoop.ts | ported |
| io/github/gabrielshanahan/scoop/utils.kt | src/utils.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/Action.kt | src/coroutine/Action.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/ActionInput.kt | src/coroutine/ActionInput.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/ActionTopic.kt | src/coroutine/ActionTopic.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/CooperationScope.kt | src/coroutine/CooperationScope.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/CooperationScopeIdentifier.kt | src/coroutine/CooperationScopeIdentifier.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/DistributedCoroutine.kt | src/coroutine/DistributedCoroutine.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/DistributedCoroutineIdentifier.kt | src/coroutine/DistributedCoroutineIdentifier.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/EventLoop.kt | src/coroutine/EventLoop.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/Handler.kt | src/coroutine/Handler.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/PeriodicTick.kt | src/coroutine/PeriodicTick.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/ReconcileGate.kt | src/coroutine/ReconcileGate.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/ScoopException.kt | src/coroutine/ScoopException.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/ScoopInfrastructureException.kt | src/coroutine/ScoopInfrastructureException.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/Topic.kt | src/coroutine/Topic.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/TransactionRunner.kt | src/coroutine/TransactionRunner.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/VariableName.kt | src/coroutine/VariableName.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/context/CancellationToken.kt | src/coroutine/context/CancellationToken.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/context/CooperationContext.kt | src/coroutine/context/CooperationContext.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/context/CooperationContextMap.kt | src/coroutine/context/CooperationContextMap.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/context/CooperationContextModule.kt | src/coroutine/context/CooperationContextModule.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/context/util.kt | src/coroutine/context/util.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/Capabilities.kt | src/coroutine/structuredcooperation/Capabilities.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/CooperationFailure.kt | src/coroutine/structuredcooperation/CooperationFailure.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/MessageEventRepository.kt | src/coroutine/structuredcooperation/MessageEventRepository.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/PendingCoroutineRunSql.kt | src/coroutine/structuredcooperation/PendingCoroutineRunSql.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/ReturnValueAlreadyExistsException.kt | src/coroutine/structuredcooperation/ReturnValueAlreadyExistsException.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/ReturnValueRepository.kt | src/coroutine/structuredcooperation/ReturnValueRepository.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/exceptions.kt | src/coroutine/structuredcooperation/exceptions.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/RollbackState.kt | src/coroutine/eventloop/RollbackState.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/SuspensionState.kt | src/coroutine/eventloop/SuspensionState.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/exceptions.kt | src/coroutine/eventloop/exceptions.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/deadline/AbsoluteDeadline.kt | src/coroutine/eventloop/deadline/AbsoluteDeadline.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/deadline/Deadline.kt | src/coroutine/eventloop/deadline/Deadline.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/deadline/HappyPathDeadline.kt | src/coroutine/eventloop/deadline/HappyPathDeadline.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/deadline/RollbackPathDeadline.kt | src/coroutine/eventloop/deadline/RollbackPathDeadline.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/strategy/EventLoopStrategy.kt | src/coroutine/eventloop/strategy/EventLoopStrategy.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/strategy/StandardEventLoopStrategy.kt | src/coroutine/eventloop/strategy/StandardEventLoopStrategy.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/eventloop/strategy/strategyBuilders.kt | src/coroutine/eventloop/strategy/strategyBuilders.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/continuation/Continuation.kt | src/coroutine/continuation/Continuation.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/continuation/ContinuationIdentifier.kt | src/coroutine/continuation/ContinuationIdentifier.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/continuation/CooperationContinuation.kt | src/coroutine/continuation/CooperationContinuation.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/continuation/HappyPathContinuation.kt | src/coroutine/continuation/HappyPathContinuation.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/continuation/RollbackPathContinuation.kt | src/coroutine/continuation/RollbackPathContinuation.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/builder/SagaBuilder.kt | src/coroutine/builder/SagaBuilder.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/builder/SagaBuilderExtensions.kt | src/coroutine/builder/SagaBuilderExtensions.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/builder/Sleep.kt | src/coroutine/builder/Sleep.ts | ported |
| io/github/gabrielshanahan/scoop/coroutine/builder/TryFinally.kt | src/coroutine/builder/TryFinally.ts | ported |
| io/github/gabrielshanahan/scoop/util/UuidV7.kt | src/util/UuidV7.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/HandlerRegistry.kt | src/messaging/HandlerRegistry.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/Message.kt | src/messaging/Message.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/MessageQueueExtensions.kt | src/messaging/MessageQueueExtensions.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/MessageRepository.kt | src/messaging/MessageRepository.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/PostgresMessageQueue.kt | src/messaging/PostgresMessageQueue.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/Subscription.kt | src/messaging/Subscription.ts | ported |
| io/github/gabrielshanahan/scoop/messaging/TopicNotifier.kt | src/messaging/TopicNotifier.ts | ported |

## scoop-quarkus main sources (5 files)

Each maps to a core/node concern per DECISIONS.md ("The scoop-quarkus question").

| Kotlin file | TS counterpart | Status |
|---|---|---|
| io/github/gabrielshanahan/scoop/quarkus/JtaTransactionRunner.kt | see DECISIONS.md | ported |
| io/github/gabrielshanahan/scoop/quarkus/PgSubscriberProducer.kt | see DECISIONS.md | ported |
| io/github/gabrielshanahan/scoop/quarkus/PgSubscriberTopicNotifier.kt | see DECISIONS.md | ported |
| io/github/gabrielshanahan/scoop/quarkus/QuarkusCooperationContextCustomizer.kt | see DECISIONS.md | ported |
| io/github/gabrielshanahan/scoop/quarkus/ScoopProducer.kt | see DECISIONS.md | ported |

## Tests (195 test methods across 29 files, of which 4 are helpers/base classes with no @Test methods)

### io/github/gabrielshanahan/scoop/coroutine/ReconcileGateTest.kt (7 tests) → test/coroutine/ReconcileGateTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | ALWAYS always reconciles regardless of state | verified | |
| 2 | starts armed so the first tick reconciles, then drains to idle | verified | |
| 3 | markDirty re-arms the drain | verified | |
| 4 | a productive pass keeps the drain going (handles contending siblings) | verified | |
| 5 | safety net forces a reconcile when idle | verified | |
| 6 | reconcileFailed re-arms so the next tick retries | verified | |
| 7 | a notification landing during a reconcile survives the consume-before-work clear | verified | |

### io/github/gabrielshanahan/scoop/coroutine/context/CooperationContextModuleTest.kt (62 tests) → test/coroutine/context/CooperationContextModuleTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | tab character | verified | |
| 2 | newline character | verified | |
| 3 | carriage return | verified | |
| 4 | carriage return plus newline | verified | |
| 5 | escaped double quotes | verified | |
| 6 | backslashes | verified | |
| 7 | forward slash | verified | |
| 8 | backspace character | verified | |
| 9 | form feed character | verified | |
| 10 | null byte | verified | |
| 11 | all JSON escape sequences combined | verified | |
| 12 | multiple special chars in sequence | verified | |
| 13 | special chars preserve typed value | verified | |
| 14 | basic multilingual plane - CJK characters | verified | |
| 15 | emoji | verified | |
| 16 | arabic text | verified | |
| 17 | mixed ascii and unicode | verified | |
| 18 | unicode control characters | verified | |
| 19 | zero-width characters | verified | |
| 20 | empty string | verified | |
| 21 | string with only spaces | verified | |
| 22 | very long string | verified | |
| 23 | long string with special chars throughout | verified | |
| 24 | string that looks like JSON | verified | |
| 25 | string that looks like a number | verified | |
| 26 | string that looks like boolean | verified | |
| 27 | string that looks like null | verified | |
| 28 | positive integers | verified | |
| 29 | zero | verified | |
| 30 | negative numbers | verified | |
| 31 | large numbers | verified | |
| 32 | min value numbers | verified | |
| 33 | true value | verified | |
| 34 | false value | verified | |
| 35 | null field value | verified | |
| 36 | non-null field value | verified | |
| 37 | mixed null and non-null fields | verified | |
| 38 | empty list | verified | |
| 39 | single item list | verified | |
| 40 | multi item list | verified | |
| 41 | list with special characters in items | verified | |
| 42 | list with empty strings | verified | |
| 43 | nested list via map | verified | |
| 44 | simple nested object | verified | |
| 45 | nested object with special chars | verified | |
| 46 | deeply nested objects | verified | |
| 47 | deeply nested with special chars | verified | |
| 48 | empty map | verified | |
| 49 | map with various value types | verified | |
| 50 | map with nested map | verified | |
| 51 | simple object | verified | |
| 52 | object with special chars in values | verified | |
| 53 | nested object | verified | |
| 54 | array value | verified | |
| 55 | complex mixed structure | verified | |
| 56 | string with all escape sequences | verified | |
| 57 | two elements combined via plus | verified | |
| 58 | three elements combined | verified | |
| 59 | complex element plus simple element | verified | |
| 60 | string with special chars survives double round-trip | verified | |
| 61 | complex nested structure survives double round-trip | verified | |
| 62 | multiple elements survive double round-trip | verified | |

### io/github/gabrielshanahan/scoop/coroutine/context/CooperationContextPolymorphicCollectionTest.kt (1 tests) → test/coroutine/context/CooperationContextPolymorphicCollectionTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | polymorphic list inside MappedElement round-trips through CooperationContext | verified | |

### io/github/gabrielshanahan/scoop/quarkus/PgSubscriberTopicNotifierTest.kt (1 tests) → test/quarkus/PgSubscriberTopicNotifierTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | callback should not run on vert-x event loop thread | verified | Vert.x off-event-loop-thread dispatch → postgres.js LISTEN/NOTIFY delivery + async (microtask) dispatch; no threads on this runtime |

### io/github/gabrielshanahan/scoop/coroutine/StructuredCooperationTest.kt — helper/base class (0 tests) → status: ported (test/support/harness.ts)

### io/github/gabrielshanahan/scoop/coroutine/util.kt — helper/base class (0 tests) → status: ported (test/support/util.ts + latch.ts)

### io/github/gabrielshanahan/scoop/coroutine/context/ContextPropagationTest.kt (1 tests) → test/coroutine/context/ContextPropagationTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | context is propagated correctly | pending | |

### io/github/gabrielshanahan/scoop/coroutine/context/CooperationContextTest.kt (6 tests) → test/coroutine/context/CooperationContextTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | test emtpy | pending | |
| 2 | test null values are preserved in round-trip | pending | |
| 3 | test null values mixed with other values are preserved in round-trip | pending | |
| 4 | test nested null values are preserved in round-trip | pending | |
| 5 | test null values in arrays are preserved in round-trip | pending | |
| 6 | test everything works as expected | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/CancellationTest.kt (2 tests) → test/coroutine/structuredcooperation/CancellationTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | cancellation works | verified | |
| 2 | cancellation after everything has finished running has no effect | verified | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/CooperationFailureTest.kt (2 tests) → test/coroutine/structuredcooperation/CooperationFailureTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | test deserialization of unknown failure type | verified | exact-JSON pin (JVM/Quarkus frames) → structural pin of serialized form; frame values are environment paths on this runtime |
| 2 | test idempotence of mapping Throwable - CooperationFailure - JSON - CooperationFailure - Throwable | verified | exact-JSON pin (JVM/Quarkus frames) → structural pin of serialized form; frame values are environment paths on this runtime |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/GoToTest.kt (10 tests) → test/coroutine/structuredcooperation/GoToTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | GoTo forward skips intermediate steps | verified | |
| 2 | GoTo backward re-executes from target step | verified | |
| 3 | GoTo self behaves like Repeat | verified | |
| 4 | GoTo plus rollback rolls back in reverse chronological order | verified | |
| 5 | loop with children rolls back each iteration's children in reverse | verified | |
| 6 | handleChildFailures can override NextStep and childFailureHandlerIteration increments correctly | verified | |
| 7 | handleChildFailures receives correct nextStep for GoTo | verified | |
| 8 | GoTo forward skip does not include skipped step in rollback | verified | |
| 9 | GoTo forward then GoTo backward with failure rolls back all visited instances | verified | |
| 10 | GoTo to repeating step then failure rolls back all instances | verified | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/HappyPathTest.kt (4 tests) → test/coroutine/structuredcooperation/HappyPathTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | handler should not complete until handlers listening to emitted messages complete - depth 1 | verified | |
| 2 | handler should not complete until handlers listening to emitted messages complete - depth 2 | verified | |
| 3 | multiple handlers at same level should all complete before parent handler completes | verified | |
| 4 | parent should wait for multiple handlers listening to the same topic | verified | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/InfrastructureFailureRetryTest.kt (2 tests) → test/coroutine/structuredcooperation/InfrastructureFailureRetryTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | a ScoopInfrastructureException is retried (not rolled back) and the run eventually commits | pending | |
| 2 | a plain business exception still rolls back and is not retried | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/JtaAtomicityTest.kt (2 tests) → test/coroutine/structuredcooperation/JtaAtomicityTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | business write inside a step commits atomically with scoop events | pending | |
| 2 | business write rolls back together with the step when the step throws | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/LoopTest.kt (9 tests) → test/coroutine/structuredcooperation/LoopTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | step returning Repeat re-executes with incremented iteration | verified | |
| 2 | zero-iteration loop behaves like normal step | verified | |
| 3 | loop with child launches waits for each batch | verified | |
| 4 | handleChildFailures receives correct childFailureHandlerIteration | verified | |
| 5 | mid-loop failure triggers rollback for each iteration in reverse order | verified | |
| 6 | single iteration loop with rollback behaves like normal step rollback | verified | |
| 7 | completed loop followed by later failure rolls back all iterations | verified | |
| 8 | multiple loop steps followed by failure roll back all iterations of both | verified | |
| 9 | childFailureHandlerIteration is tracked correctly during rollback | verified | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/MessageEventsTest.kt (8 tests) → test/coroutine/structuredcooperation/MessageEventsTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | should write EMITTED event when message is published | pending | |
| 2 | should write one SEEN event per handler | pending | |
| 3 | should synchronize multiple instances of the same handler using message event records | pending | |
| 4 | should write COMMITTED event on successful transaction | pending | |
| 5 | should write ROLLED_BACK event when exception is thrown | pending | |
| 6 | should follow complete message event writing sequence on successful processing | pending | |
| 7 | should follow complete message event writing sequence on failed processing | pending | |
| 8 | multiple handler instances should coordinate using message events for multiple messages | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/PendingCoroutineRunSqlTest.kt (44 tests) → test/coroutine/structuredcooperation/PendingCoroutineRunSqlTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | picks up SEEN | pending | |
| 2 | picks up SUSPENDED | pending | |
| 3 | picks up ROLLING_BACK | pending | |
| 4 | picks up combinations | pending | |
| 5 | picks up COMMITTED with parent ROLLBACK_EMITTED | pending | |
| 6 | does not pick up COMMITED without parent ROLLING_BACK | pending | |
| 7 | does not pick up rolled back | pending | |
| 8 | does not pick up rollback failed | pending | |
| 9 | picks up latest suspended | pending | |
| 10 | picks up latest suspended after rollback | pending | |
| 11 | works when no suspended is present | pending | |
| 12 | picks up child emissions in last step | pending | |
| 13 | does nothing when no emissions | pending | |
| 14 | picks up child seens and their terminations | pending | |
| 15 | picks up rollback emissions in last step | pending | |
| 16 | does nothing when no emissions | pending | |
| 17 | picks up child rolling backs and their terminations | pending | |
| 18 | picks up SEEN | pending | |
| 19 | picks up SUSPENDED | pending | |
| 20 | picks up ROLLING_BACK | pending | |
| 21 | picks up combinations | pending | |
| 22 | picks up when children committed | pending | |
| 23 | picks up when children rolled back | pending | |
| 24 | picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK | pending | |
| 25 | doesn't pick up when there are unfinished children emissions | pending | |
| 26 | doesn't pick up when there are unfinished children emissions - 2 deep | pending | |
| 27 | picks up SEEN | pending | |
| 28 | picks up SUSPENDED | pending | |
| 29 | picks up ROLLING_BACK | pending | |
| 30 | picks up combinations | pending | |
| 31 | picks up when all children finished | pending | |
| 32 | picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK | pending | |
| 33 | doesn't pick up when no child SEENs are present | pending | |
| 34 | doesn't pick up when a child SEEN is missing, even if the rest are finished | pending | |
| 35 | doesn't pick up when no child ROLLING_BACKs are present | pending | |
| 36 | doesn't pick up when a child ROLLING_BACK is missing, even if the rest are finished | pending | |
| 37 | when present, the time of rollback emission time determines precedence | pending | |
| 38 | otherwise, first emitted first processed | pending | |
| 39 | only single transaction can pick up a SEEN | pending | |
| 40 | happy path | pending | |
| 41 | picks up children rolling back | pending | |
| 42 | picks up children rollback failures | pending | |
| 43 | picks up rolling backs just starting | pending | |
| 44 | picks up rolling backs later on | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/ReturnValueTest.kt (4 tests) → test/coroutine/structuredcooperation/ReturnValueTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | child handler can store a return value that parent retrieves | pending | |
| 2 | multiple child handlers can each store return values | pending | |
| 3 | getReturnValue retrieves a specific child's return value by handler | pending | |
| 4 | different variable names are independent | pending | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/RollbackPathTest.kt (10 tests) → test/coroutine/structuredcooperation/RollbackPathTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | a handler failing in its first step should never emit what is in the step and not call rollback() (since the transaction wasn't committed) | verified | |
| 2 | a handler failing in its second step should emit ROLLBACK_EMITTEDs for messages emitted in the first step, and then roll it back | verified | |
| 3 | when a child fails, rollbacks happen in reverse order | verified | |
| 4 | when a later step fails, previous emissions are rolled back | verified | |
| 5 | rollbacks are well behaved n-deep | verified | |
| 6 | failed rollbacks are well behaved n-deep | verified | |
| 7 | when stuff is emitted in handleChildFailures and then a rollback happens, all things that haven't already been rolled back are rolled back | verified | |
| 8 | rolling back the entire hierarchy works | verified | |
| 9 | rolling back sub-hierarchy works (but should be done carefully, as you run the risk of bringing the state of the system into an inconsistent state from a business perspective) | verified | |
| 10 | rolling back while things are still running has no effect | verified | |

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/SqlTestUtils.kt — helper/base class (0 tests) → status: ported (test/support/SqlTestUtils.ts)

### io/github/gabrielshanahan/scoop/coroutine/structuredcooperation/StubHandlerBlockingTest.kt (2 tests) → test/coroutine/structuredcooperation/StubHandlerBlockingTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | repeating step blocks when stub handler has not started | pending | |
| 2 | repeating step resumes after externally writing SEEN and COMMITTED for stub handler | pending | |

### io/github/gabrielshanahan/scoop/coroutine/eventloop/deadline/DeadlineTest.kt (1 tests) → test/coroutine/eventloop/deadline/DeadlineTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | happy path deadlines work | pending | |

### io/github/gabrielshanahan/scoop/coroutine/builder/SleepTest.kt (3 tests) → test/coroutine/builder/SleepTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | sleep works | pending | |
| 2 | scheduling works | pending | |
| 3 | periodic scheduling works | pending | |

### io/github/gabrielshanahan/scoop/coroutine/builder/TryFinallyTest.kt (5 tests) → test/coroutine/builder/TryFinallyTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | finally is executed on success | pending | |
| 2 | finally is executed on root failure but messages are not emitted, because neither were those in the 'try' step | pending | |
| 3 | finally is executed on child failure | pending | |
| 4 | finally is executed, once, on subsequent step failure | pending | |
| 5 | finally is only executed once when its child causes a rollback | pending | |

### io/github/gabrielshanahan/scoop/messaging/MultiHandlerTopicNotifyTest.kt (1 tests) → test/messaging/MultiHandlerTopicNotifyTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | both sagas on one topic are notified promptly by a single message | verified | |

### io/github/gabrielshanahan/scoop/messaging/PostgresMessageQueueExceptionTest.kt (1 tests) → test/messaging/PostgresMessageQueueExceptionTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | test exception is stored and can be retrieved | verified | JVM FQCN exception type → Error.name ('CustomTestException') |

### io/github/gabrielshanahan/scoop/messaging/PostgresMessageQueueTest.kt (6 tests) → test/messaging/PostgresMessageQueueTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | should publish a message | verified | |
| 2 | should subscribe to messages | verified | |
| 3 | subscribe should isolate transactions between messages and correctly roll back failures | verified | |
| 4 | subscribe with multiple instances fans work out across distinct instance UUIDs | verified | |
| 5 | subscribe rejects instances less than one | verified | |
| 6 | requiredConnectionCount reflects registered worker instances | verified | |

### io/github/gabrielshanahan/scoop/messaging/shutdownrepro/ReproSubscriptionRegistrar.kt — helper/base class (0 tests) → status: ported (test/messaging/shutdownrepro/ReproSubscriptionRegistrar.ts)

### io/github/gabrielshanahan/scoop/messaging/shutdownrepro/ShutdownSpamReproTest.kt (1 tests) → test/messaging/shutdownrepro/ShutdownSpamReproTest.test.ts

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | subscriptions leak past test end so quarkus shutdown races scoop ticks | verified | Quarkus-shutdown log grep → stdout capture across dedicated-instance teardown; asserts zero tick-error spam |

## Reconciliation

- scoop-core main files: 58
- scoop-quarkus main files: 5
- test files: 29 (4 helpers)
- test methods: 195

Run `npx tsx scripts/reconcile.ts` to mechanically verify this ledger against the reference repo and the ported suite.
