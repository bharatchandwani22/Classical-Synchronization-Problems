#!/usr/bin/env python3
"""
Classical Synchronization Problems (Python threads)
===================================================
1. Producer-Consumer      (finite circular buffer: Semaphore + Lock)
2. Readers-Writers        (protected reader count, exclusive writers: Lock)
3. Dining Philosophers    (resource ordering / waiter semaphore / deadlock demo)

Every problem is run with the SAME workload twice:
    * UNSYNCHRONIZED  -> shows race conditions (expected != actual)
    * SYNCHRONIZED    -> expected == actual

All runs use finite iterations and a watchdog join timeout, so every program
terminates normally (no permanent deadlock).

Usage:  python sync_problems.py [pc | rw | dp | all]
"""

import sys
import time
import random
import threading
from collections import Counter

# ----------------------------------------------------------------------------
# Common helpers
# ----------------------------------------------------------------------------
_print_lock = threading.Lock()
SUMMARY = []          # (problem, variant, is_synchronized, correct, details)
WATCHDOG_SECONDS = 30


def say(msg):
    """Thread-safe printing so output lines never interleave."""
    with _print_lock:
        print(msg, flush=True)


def banner(title):
    say("\n" + "=" * 78)
    say(title)
    say("=" * 78)


def jitter(max_s=0.003):
    time.sleep(random.uniform(0, max_s))


def run_threads(threads, timeout=WATCHDOG_SECONDS):
    """Start threads, join with a global deadline. Returns True if all finished."""
    for t in threads:
        t.start()
    deadline = time.time() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.time()))
    alive = [t.name for t in threads if t.is_alive()]
    if alive:
        say(f"WATCHDOG: threads still running after {timeout}s "
            f"(possible deadlock): {alive}")
    return not alive


# ============================================================================
# PROBLEM 1 : PRODUCER - CONSUMER  (finite circular buffer)
# ============================================================================
PC_BUFFER_SIZE = 4
PC_PRODUCERS = 2
PC_CONSUMERS = 2
PC_ITEMS_EACH = 6          # items per producer


class SafeCircularBuffer:
    """Bounded circular buffer.
    empty  : counting semaphore = free slots  (producer blocks when full)
    full   : counting semaphore = used slots  (consumer blocks when empty)
    mutex  : Lock giving exclusive access to buffer indices / contents
    """

    def __init__(self, size):
        self.size = size
        self.buf = [None] * size
        self.head = 0                      # next write position
        self.tail = 0                      # next read position
        self.count = 0
        self.max_count = 0
        self.empty = threading.Semaphore(size)
        self.full = threading.Semaphore(0)
        self.mutex = threading.Lock()

    def state(self):
        return f"buffer={self.buf} count={self.count}"

    def put(self, item, who):
        self.empty.acquire()                       # wait for a free slot
        with self.mutex:                           # critical section
            slot = self.head
            self.buf[slot] = item
            self.head = (self.head + 1) % self.size
            self.count += 1
            self.max_count = max(self.max_count, self.count)
            say(f"[{who}] PUT {item:>3} -> slot {slot} | {self.state()}")
        self.full.release()                        # signal: one more item

    def get(self, who):
        self.full.acquire()                        # wait for an item
        with self.mutex:
            slot = self.tail
            item = self.buf[slot]
            self.buf[slot] = None
            self.tail = (self.tail + 1) % self.size
            self.count -= 1
            say(f"[{who}] GOT {item:>3} <- slot {slot} | {self.state()}")
        self.empty.release()                       # signal: one more free slot
        return item


class UnsafeCircularBuffer:
    """Same buffer, NO synchronization: no full/empty checks, no mutex.
    Sleeps inside read-modify-write sequences widen the race window."""

    def __init__(self, size):
        self.size = size
        self.buf = [None] * size
        self.head = 0
        self.tail = 0
        self.count = 0
        self.max_count = 0

    def state(self):
        return f"buffer={self.buf} count={self.count}"

    def put(self, item, who):
        slot = self.head
        jitter(0.002)
        self.buf[slot] = item                      # may overwrite unread data
        self.head = (slot + 1) % self.size
        c = self.count
        jitter(0.002)
        self.count = c + 1                         # lost-update race
        self.max_count = max(self.max_count, self.count)
        say(f"[{who}] PUT {item:>3} -> slot {slot} | {self.state()}")

    def get(self, who):
        slot = self.tail
        jitter(0.002)
        item = self.buf[slot]                      # may read None / duplicate
        self.buf[slot] = None
        self.tail = (slot + 1) % self.size
        c = self.count
        jitter(0.002)
        self.count = c - 1
        say(f"[{who}] GOT {str(item):>3} <- slot {slot} | {self.state()}")
        return item


def pc_producer(buf, pid):
    for i in range(PC_ITEMS_EACH):
        jitter()
        buf.put(pid * 100 + i + 1, f"P{pid}")


def pc_consumer(buf, cid, n, out):
    for _ in range(n):
        jitter()
        out.append(buf.get(f"C{cid}"))


def run_producer_consumer(synchronized):
    label = "SYNCHRONIZED" if synchronized else "UNSYNCHRONIZED"
    banner(f"PRODUCER-CONSUMER [{label}]  buffer={PC_BUFFER_SIZE} "
           f"producers={PC_PRODUCERS} consumers={PC_CONSUMERS} "
           f"items/producer={PC_ITEMS_EACH}")
    random.seed(1)
    buf = (SafeCircularBuffer if synchronized else UnsafeCircularBuffer)(PC_BUFFER_SIZE)
    total = PC_PRODUCERS * PC_ITEMS_EACH
    per_consumer = total // PC_CONSUMERS
    consumed = {c: [] for c in range(1, PC_CONSUMERS + 1)}

    threads = [threading.Thread(target=pc_producer, args=(buf, p),
                                name=f"P{p}", daemon=True)
               for p in range(1, PC_PRODUCERS + 1)]
    threads += [threading.Thread(target=pc_consumer,
                                 args=(buf, c, per_consumer, consumed[c]),
                                 name=f"C{c}", daemon=True)
                for c in range(1, PC_CONSUMERS + 1)]
    finished = run_threads(threads)

    expected = {p * 100 + i + 1 for p in range(1, PC_PRODUCERS + 1)
                for i in range(PC_ITEMS_EACH)}
    flat = [x for lst in consumed.values() for x in lst]
    valid = [x for x in flat if x is not None]
    nones = len(flat) - len(valid)
    cnt = Counter(valid)
    lost = sorted(expected - set(cnt))
    dups = sorted(k for k, v in cnt.items() if v > 1)

    say("-" * 78)
    say(f"EXPECTED : {total} distinct items consumed exactly once, "
        f"0 empty reads, final count = 0, count never > {PC_BUFFER_SIZE}")
    say(f"ACTUAL   : {len(valid)} valid reads, {len(cnt)} distinct, "
        f"{nones} empty(None) reads, final count = {buf.count}, "
        f"max count = {buf.max_count}")
    say(f"           lost items = {lost}")
    say(f"           duplicated items = {dups}")
    ok = (finished and not lost and not dups and nones == 0
          and buf.count == 0 and buf.max_count <= PC_BUFFER_SIZE
          and len(valid) == total)
    say(f"RESULT   : {'CORRECT' if ok else 'INCORRECT (race condition detected)'}")
    SUMMARY.append(("Producer-Consumer", label, synchronized, ok,
                    f"lost={len(lost)} dup={len(dups)} empty_reads={nones} "
                    f"final_count={buf.count}"))


# ============================================================================
# PROBLEM 2 : READERS - WRITERS
# ============================================================================
RW_READERS = 4
RW_READS = 5              # reads per reader
RW_WRITERS = 2
RW_WRITES = 5             # writes per writer


class SharedRecord:
    """Two fields a writer must always keep equal (a == b invariant)."""

    def __init__(self):
        self.a = 0
        self.b = 0


class RWMonitor:
    """Instrumentation only (NOT access control): counts active threads and
    detects invariant violations. Has its own lock so counters are exact."""

    def __init__(self):
        self.lock = threading.Lock()
        self.readers = 0
        self.writers = 0
        self.max_readers = 0
        self.violations = 0        # reader+writer or writer+writer overlap
        self.inconsistent = 0      # reader saw a != b

    def enter_read(self):
        with self.lock:
            self.readers += 1
            self.max_readers = max(self.max_readers, self.readers)
            if self.writers > 0:
                self.violations += 1

    def exit_read(self, consistent):
        with self.lock:
            self.readers -= 1
            if not consistent:
                self.inconsistent += 1

    def enter_write(self):
        with self.lock:
            if self.writers > 0 or self.readers > 0:
                self.violations += 1
            self.writers += 1

    def exit_write(self):
        with self.lock:
            self.writers -= 1

    def snapshot(self):
        with self.lock:
            return f"active readers={self.readers} writers={self.writers}"


class NoLock:
    """Unsynchronized: every operation is a no-op."""
    def start_read(self): pass
    def end_read(self): pass
    def start_write(self): pass
    def end_write(self): pass


class ReaderPreferenceLock:
    """Classic first readers-writers solution.
    mutex    protects read_count
    resource gives writers exclusive access; the FIRST reader locks it for the
             whole group of readers, the LAST reader releases it.
    Readers can starve writers (writer waits while new readers keep arriving)."""

    def __init__(self):
        self.mutex = threading.Lock()
        self.resource = threading.Lock()
        self.read_count = 0

    def start_read(self):
        with self.mutex:
            self.read_count += 1
            if self.read_count == 1:
                self.resource.acquire()

    def end_read(self):
        with self.mutex:
            self.read_count -= 1
            if self.read_count == 0:
                self.resource.release()

    def start_write(self):
        self.resource.acquire()

    def end_write(self):
        self.resource.release()


class FairLock(ReaderPreferenceLock):
    """Adds a turnstile: a waiting writer holds it, so NEW readers queue behind
    the writer. Existing readers drain, the writer runs, then readers resume.
    This removes writer starvation."""

    def __init__(self):
        super().__init__()
        self.turnstile = threading.Lock()

    def start_read(self):
        with self.turnstile:            # pass through (blocked if writer waits)
            pass
        super().start_read()

    def start_write(self):
        self.turnstile.acquire()        # block new readers
        self.resource.acquire()         # wait for current readers to leave
        self.turnstile.release()


def rw_reader(rid, rw, rec, mon):
    for _ in range(RW_READS):
        jitter(0.004)
        rw.start_read()
        mon.enter_read()
        a = rec.a
        time.sleep(0.003)               # long read widens the race window
        b = rec.b
        ok = (a == b)
        say(f"[R{rid}] read a={a} b={b} {'consistent' if ok else 'INCONSISTENT'}"
            f" | {mon.snapshot()}")
        mon.exit_read(ok)
        rw.end_read()


def rw_writer(wid, rw, rec, mon, waits):
    for _ in range(RW_WRITES):
        jitter(0.004)
        t0 = time.time()
        rw.start_write()
        waits.append(time.time() - t0)
        mon.enter_write()
        v = rec.a
        time.sleep(0.002)
        rec.a = v + 1                   # read-modify-write on a
        time.sleep(0.002)
        w = rec.b
        time.sleep(0.002)
        rec.b = w + 1                   # read-modify-write on b
        say(f"[W{wid}] wrote a={rec.a} b={rec.b} | {mon.snapshot()}")
        mon.exit_write()
        rw.end_write()


def run_readers_writers(name, lock_factory, synchronized):
    banner(f"READERS-WRITERS [{name}]  readers={RW_READERS}x{RW_READS} reads, "
           f"writers={RW_WRITERS}x{RW_WRITES} writes")
    random.seed(2)
    rw, rec, mon = lock_factory(), SharedRecord(), RWMonitor()
    waits = {w: [] for w in range(1, RW_WRITERS + 1)}
    threads = [threading.Thread(target=rw_reader, args=(r, rw, rec, mon),
                                name=f"R{r}", daemon=True)
               for r in range(1, RW_READERS + 1)]
    threads += [threading.Thread(target=rw_writer, args=(w, rw, rec, mon, waits[w]),
                                 name=f"W{w}", daemon=True)
                for w in range(1, RW_WRITERS + 1)]
    finished = run_threads(threads)

    exp = RW_WRITERS * RW_WRITES
    all_waits = [x for lst in waits.values() for x in lst]
    avg_w = sum(all_waits) / len(all_waits) if all_waits else 0
    max_w = max(all_waits) if all_waits else 0
    say("-" * 78)
    say(f"EXPECTED : final a = b = {exp}, 0 inconsistent reads, "
        f"0 reader/writer overlaps")
    say(f"ACTUAL   : final a = {rec.a}, b = {rec.b}, "
        f"inconsistent reads = {mon.inconsistent}, overlaps = {mon.violations}")
    say(f"           max concurrent readers = {mon.max_readers}")
    say(f"           writer wait time: avg = {avg_w*1000:.1f} ms, "
        f"max = {max_w*1000:.1f} ms")
    ok = (finished and rec.a == exp and rec.b == exp
          and mon.inconsistent == 0 and mon.violations == 0)
    say(f"RESULT   : {'CORRECT' if ok else 'INCORRECT (race condition detected)'}")
    SUMMARY.append(("Readers-Writers", name, synchronized, ok,
                    f"final a={rec.a} b={rec.b} (exp {exp}) "
                    f"inconsistent={mon.inconsistent} overlaps={mon.violations} "
                    f"max_writer_wait={max_w*1000:.0f}ms"))


# ============================================================================
# PROBLEM 3 : DINING PHILOSOPHERS
# ============================================================================
DP_N = 5
DP_MEALS = 3              # meals per philosopher


class Table:
    """Monitor: tracks who is eating; counts neighbours eating simultaneously
    (they would be sharing a fork) and meals eaten."""

    def __init__(self):
        self.lock = threading.Lock()
        self.eating = [False] * DP_N
        self.meals = [0] * DP_N
        self.violations = 0

    def start(self, i):
        with self.lock:
            if self.eating[(i - 1) % DP_N] or self.eating[(i + 1) % DP_N]:
                self.violations += 1
            self.eating[i] = True

    def stop(self, i):
        with self.lock:
            self.eating[i] = False
            self.meals[i] += 1

    def snapshot(self):
        with self.lock:
            return [i for i, e in enumerate(self.eating) if e]


def fork_state(forks):
    return "".join("X" if f.locked() else "." for f in forks)   # X = in use


def philosopher(i, table, forks, strategy, waiter, waits):
    left, right = i, (i + 1) % DP_N
    if strategy == "ordering":                  # always lower-numbered fork first
        first, second = min(left, right), max(left, right)
    else:
        first, second = left, right
    for m in range(1, DP_MEALS + 1):
        jitter(0.004)                           # think
        say(f"[Phil {i}] hungry (meal {m})")
        t0 = time.time()
        if strategy == "waiter":
            waiter.acquire()                    # at most N-1 may try to eat
        if strategy != "none":
            forks[first].acquire()
            forks[second].acquire()
        waits[i].append(time.time() - t0)
        table.start(i)
        say(f"[Phil {i}] EATING {m}/{DP_MEALS} forks({left},{right}) "
            f"| fork-state={fork_state(forks)} (X=in use) "
            f"| eating now={table.snapshot()}")
        time.sleep(0.004)                       # eat
        table.stop(i)
        if strategy != "none":
            forks[second].release()
            forks[first].release()
        if strategy == "waiter":
            waiter.release()
        say(f"[Phil {i}] done meal {m}, back to thinking")


def run_dining(strategy, title, synchronized):
    banner(f"DINING PHILOSOPHERS [{title}]  philosophers={DP_N}, "
           f"meals each={DP_MEALS}")
    random.seed(3)
    table = Table()
    forks = [threading.Lock() for _ in range(DP_N)]
    waiter = threading.Semaphore(DP_N - 1) if strategy == "waiter" else None
    waits = {i: [] for i in range(DP_N)}
    threads = [threading.Thread(target=philosopher,
                                args=(i, table, forks, strategy, waiter, waits),
                                name=f"Phil{i}", daemon=True)
               for i in range(DP_N)]
    finished = run_threads(threads)

    max_w = max((max(w) for w in waits.values() if w), default=0)
    say("-" * 78)
    say(f"EXPECTED : every philosopher eats {DP_MEALS} meals "
        f"({DP_N * DP_MEALS} total), 0 neighbours eating together, no deadlock")
    say(f"ACTUAL   : meals per philosopher = {table.meals}, "
        f"neighbour conflicts = {table.violations}, "
        f"finished = {finished}")
    say(f"           max wait for forks = {max_w*1000:.1f} ms")
    ok = (finished and all(m == DP_MEALS for m in table.meals)
          and table.violations == 0)
    say(f"RESULT   : {'CORRECT' if ok else 'INCORRECT (fork shared by two philosophers)'}")
    SUMMARY.append(("Dining Philosophers", title, synchronized, ok,
                    f"meals={table.meals} conflicts={table.violations} "
                    f"max_wait={max_w*1000:.0f}ms"))


def run_deadlock_demo():
    """Naive 'left fork then right fork' with a barrier that forces the circular
    wait. A 0.5 s acquire timeout detects the deadlock and breaks it, so the
    program still terminates."""
    banner("DINING PHILOSOPHERS [NAIVE LEFT-THEN-RIGHT: DEADLOCK DEMO, "
           "bounded by timeout]")
    forks = [threading.Lock() for _ in range(DP_N)]
    barrier = threading.Barrier(DP_N)
    outcome = {}

    def phil(i):
        left, right = i, (i + 1) % DP_N
        forks[left].acquire()
        say(f"[Phil {i}] picked LEFT fork {left}")
        barrier.wait(timeout=5)                 # everyone now holds a left fork
        say(f"[Phil {i}] requests RIGHT fork {right} (held by Phil {right})")
        if forks[right].acquire(timeout=0.5):
            say(f"[Phil {i}] got both forks and ate")
            time.sleep(0.002)
            forks[right].release()
            outcome[i] = "ate"
        else:
            say(f"[Phil {i}] TIMEOUT -> circular wait detected, "
                f"releasing left fork {left}")
            outcome[i] = "deadlock-timeout"
        forks[left].release()

    threads = [threading.Thread(target=phil, args=(i,), name=f"Phil{i}",
                                daemon=True) for i in range(DP_N)]
    finished = run_threads(threads)
    stuck = sum(1 for v in outcome.values() if v == "deadlock-timeout")
    say("-" * 78)
    say(f"EXPECTED : circular wait -> nobody can proceed without outside help")
    say(f"ACTUAL   : {stuck} of {DP_N} philosophers hit the deadlock timeout; "
        f"outcomes = {dict(sorted(outcome.items()))}")
    say(f"RESULT   : deadlock {'DEMONSTRATED' if stuck else 'not observed'} "
        f"and broken by timeout; program finished = {finished}")
    SUMMARY.append(("Dining Philosophers", "Naive deadlock demo", False,
                    stuck > 0, f"{stuck}/{DP_N} philosophers deadlocked "
                               f"until timeout"))


# ============================================================================
# MAIN
# ============================================================================
def print_summary():
    banner("SUMMARY OF ALL RUNS")
    say(f"{'Problem':<20}{'Variant':<34}{'Sync?':<7}{'Correct?':<10}Details")
    say("-" * 120)
    for prob, var, sync, ok, det in SUMMARY:
        say(f"{prob:<20}{var:<34}{'yes' if sync else 'no':<7}"
            f"{'yes' if ok else 'NO':<10}{det}")
    bad = [s for s in SUMMARY if s[2] and not s[3]]
    say("\nAll synchronized variants correct: " + ("YES" if not bad else "NO"))
    return 0 if not bad else 1


def main():
    which = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    if which in ("pc", "all"):
        run_producer_consumer(synchronized=False)
        run_producer_consumer(synchronized=True)
    if which in ("rw", "all"):
        run_readers_writers("UNSYNCHRONIZED", NoLock, False)
        run_readers_writers("SYNCHRONIZED (reader preference)",
                            ReaderPreferenceLock, True)
        run_readers_writers("SYNCHRONIZED (fair / turnstile)", FairLock, True)
    if which in ("dp", "all"):
        run_dining("none", "UNSYNCHRONIZED (no fork locks)", False)
        run_deadlock_demo()
        run_dining("ordering", "SYNCHRONIZED (resource ordering)", True)
        run_dining("waiter", "SYNCHRONIZED (waiter: N-1 at table)", True)
    return print_summary()


if __name__ == "__main__":
    sys.exit(main())
