# Classical Synchronization Problems (Python threads)

Run: `python sync_problems.py [pc | rw | dp | all]` (Python 3.8+, standard library only).
Evidence: `test_output.txt` (full thread trace + summary of one run). The program was run 3 times; all synchronized variants were correct every time.

Every problem runs the **same workload** twice (unsynchronized, then synchronized). Every run uses finite loops plus a watchdog `join` timeout, so the program always terminates.

## Design summary

| Problem | Workload | Mechanism |
|---|---|---|
| Producer-Consumer | 2 producers x 6 items, 2 consumers x 6 items, circular buffer of 4 slots | `Semaphore(size)` = empty slots, `Semaphore(0)` = full slots, `Lock` = buffer indices/contents |
| Readers-Writers | 4 readers x 5 reads, 2 writers x 5 writes on a record with invariant `a == b` | `Lock` protects `read_count`; `Lock` `resource` gives writers exclusive access (first reader locks, last reader unlocks). Second variant adds a turnstile `Lock` for fairness |
| Dining Philosophers | 5 philosophers x 3 meals, 5 fork `Lock`s | Resource ordering (lower-numbered fork first) and a waiter `Semaphore(N-1)` (restricted concurrency). A naive left-then-right version demonstrates deadlock, broken by an acquire timeout |

## Expected vs actual (from `test_output.txt`)

| Problem | Variant | Expected | Actual |
|---|---|---|---|
| Producer-Consumer | Unsynchronized | 12 items consumed once, final count 0 | 6 items lost, 6 reads of an empty slot (`None`), final count -2 |
| Producer-Consumer | Synchronized | same | 0 lost, 0 duplicates, 0 empty reads, final count 0, never above 4 |
| Readers-Writers | Unsynchronized | `a = b = 10`, no overlaps | `a = b = 5` (half the updates lost), 27 reader/writer overlaps |
| Readers-Writers | Synchronized (both) | same | `a = b = 10`, 0 overlaps, 0 inconsistent reads |
| Dining Philosophers | No fork locks | 0 neighbours eating together | 11 conflicts (two philosophers using the same fork) |
| Dining Philosophers | Naive left-then-right | progress | 5/5 philosophers blocked until the timeout fired (deadlock) |
| Dining Philosophers | Ordering / Waiter | 3 meals each, 0 conflicts | `[3,3,3,3,3]`, 0 conflicts |

The exact numbers vary slightly between runs because thread scheduling is non-deterministic. In some runs the unsynchronized readers also saw inconsistent `a != b` snapshots (3 in run 2). The synchronized variants were correct every time.

## Discussion

### Race conditions
A race condition occurs when the result depends on the interleaving of threads that access shared data without coordination. Here:
- **Producer-Consumer:** `count += 1` is really read, add, write. Two threads read the same old value and one update is lost (hence a negative or wrong final count). Two producers also read the same `head`, write the same slot and overwrite unread items (lost items), and consumers read slots that were never filled (`None`).
- **Readers-Writers:** two writers both read `a = 3` and both write `4`, losing an update. A reader can also run between the writer's update of `a` and `b` and see `a != b` (a torn read).
- **Dining Philosophers:** without fork locks, neighbouring philosophers use the same fork at once.

### Synchronization
- **Producer-Consumer:** the two counting semaphores make threads *block* instead of overwrite or read garbage. `empty` stops producers when the buffer is full and `full` stops consumers when it is empty. The mutex makes the index and count updates atomic. The semaphore is acquired *before* the mutex; reversing that order would let a thread sleep while holding the mutex and deadlock.
- **Readers-Writers:** `mutex` makes the `read_count` update atomic, so exactly one reader (the first) locks `resource` and exactly one (the last) releases it. Readers share the resource (max 4 concurrent readers observed); writers are exclusive (0 overlaps).
- **Dining Philosophers:** each fork is a `Lock`, so a fork has at most one holder.

### Deadlock
- Producer-Consumer and Readers-Writers cannot deadlock here: locks are always taken in one fixed order and never held while waiting on something that needs them.
- Dining Philosophers has the classic circular wait. The demo forces every philosopher to hold their left fork and request the right one: all four Coffman conditions hold (mutual exclusion, hold and wait, no preemption, circular wait). The 0.5 s timeout detects it so the demo terminates.
- **Resource ordering** breaks circular wait: philosopher 4 takes fork 0 before fork 4, so the cycle cannot close.
- **Waiter (`Semaphore(N-1)`)** is restricted concurrency: with at most 4 philosophers competing for 5 forks, at least one can always get both forks.

### Starvation and fairness
- The **reader-preference** Readers-Writers lock can starve writers: while at least one reader is active the resource stays locked, so a continuous stream of new readers can postpone a writer indefinitely. Readers never starve.
- The **fair variant** adds a turnstile. A waiting writer holds it, so new readers queue behind the writer, existing readers drain, the writer runs, then readers resume. Neither side can be postponed forever.
- In this small, finite workload starvation did **not** actually appear: maximum writer wait was about 10 ms in both variants, because readers stop after 5 reads. The risk is a property of the algorithm under a continuous reader load, not something this test demonstrates. Python's `Lock` is also not guaranteed FIFO, so the fairness is "no indefinite postponement by readers", not strict arrival order.
- Dining Philosophers: with ordering and waiter, every philosopher completed all 3 meals (`[3,3,3,3,3]`) and the maximum wait for forks was about 8 ms, so no philosopher starved. Neither strategy strictly guarantees fairness (OS scheduling decides who gets a released fork first), but bounded meals and no-deadlock make progress certain here.

### Correctness
A synchronized run is judged correct only if all of these hold: all threads finish (watchdog), expected totals equal actual totals, and the invariant monitors report zero violations (no lost or duplicated items, no `a != b`, no reader/writer overlap, no two neighbours eating together). The monitor classes use their own locks and only observe; they do not provide the synchronization being tested.
