You are analyzing a Java thread dump to identify deadlocks, performance issues, and thread contention.

## Step 1: Capture the thread dump

If the user provided a file path, skip to Step 2.

If no file path was provided, help capture a thread dump from a running Gradle worker JVM:

1. Find Gradle worker processes:
   ```bash
   jps -l | grep GradleWorkerMain
   ```

2. If multiple workers are found, ask the user which PID to analyze (or analyze all if requested)

3. Capture the thread dump:
   ```bash
   jstack <PID> > /tmp/jstack-<PID>.txt
   ```

4. Proceed to analyze the captured dump file

## Step 2: Analyze the thread dump

Launch a general-purpose Task subagent with the following instructions:

**Read and analyze the jstack thread dump at: {{arg1}} (or the file path from Step 1)**

Your analysis should identify:

1. **Deadlocks** - Any threads waiting on locks held by each other (jstack usually reports these at the top)
2. **Blocked threads** - Threads in BLOCKED state and what locks they're waiting for
3. **Lock contention** - Locks (monitors) with multiple threads waiting on them
4. **Thread states distribution** - Count of RUNNABLE, WAITING, TIMED_WAITING, BLOCKED threads
5. **Thread pool health** - Look for exhausted thread pools (e.g., all threads in pool are blocked/waiting)
6. **Suspicious patterns** - Threads stuck in the same operation, recursive locks, etc.
7. **Hot code paths** - Stack traces that appear frequently across threads

**Provide:**
- **Executive Summary** - Critical issues found (deadlocks, severe contention, etc.)
- **Deadlock Analysis** - Full details of any deadlock cycles with thread names, IDs, and lock objects
- **Thread Contention** - Locks with high contention (multiple waiters) and what threads hold them
- **Thread State Overview** - Statistics on thread states
- **Problematic Threads** - Details on blocked/waiting threads with context
- **Code Locations** - If identifiable, map issues to code locations in the codebase
- **Recommended Actions** - Specific steps to resolve the issues

Format your response in clear markdown with sections for each category. Include relevant thread IDs, lock addresses, and stack trace excerpts to support findings.
