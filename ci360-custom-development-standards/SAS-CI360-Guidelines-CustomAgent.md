# CI 360 – Custom Java Agent Guidelines

----

## 1. What Is a Custom Java Agent

A **Custom Java Agent** is a long-running service that listens to events from CI 360 and does something with them (API call, DB write, etc).

Think of it as:
> A stream processor — not an API, not a batch job.

---

## 2. How It Works (Simplified)

1. Connect to CI 360  
2. Receive events continuously  
3. Acknowledge them immediately  
4. Process them in the background  

---

## 3. Acknowledging Events (Very Important)

When CI 360 sends an event, it expects a quick acknowledgement.

If you don’t acknowledge in time:
- CI 360 may resend the event  
- You may process the same event multiple times  

Ackknowlege happens implicitly in when `processEvent()` returns true.

**Correct pattern in `processEvent()`:**
- Receive event  
- Put event on a queue / worker thread for asynchronous 
- acknowledge (`return true`)


**Wrong pattern:**
- Receive event  
- Call API / write DB  
- Then acknowledge (`return true`)

> Always acknowledge first. Processing comes after.

---

## 4. Keep the Listener Lightweight

The stream listener should do almost nothing:
- No DB calls  
- No API calls  
- No heavy logic  

Just:
- Acknowledge  
- Hand off to worker  

Example - using `java.util.concurrent.ThreadPoolExecutor` for asynchronous processing:

```java
	public boolean processEvent(String eventString) throws CI360AgentException {
		Runnable eventTask = new EventTask(eventString);
		executor.execute(eventTask); /* asynchronous event processing */

		return true; /* acknowledge event */
	}
```

---

## 5. Duplicate Events (This Will Happen)

You should expect the same event to arrive more than once.

Your system must handle this safely.

**Simple rule:**
> Processing the same event twice must not break anything.

**Typical solution:**
- Store event ID in DB  
- Add UNIQUE constraint  
- If insert fails → it's a duplicate → ignore it  


---

## 6. Threads and Load

Do not let the system run wild.

Use:
- Fixed thread pools  
- Controlled queues  

Why:
- CI 360 can send faster than your downstream systems can handle  
- Without limits → crashes, timeouts, instability  

---

## 7. Be Careful with Downstream Systems

Your agent is only as fast as:
- Your database  
- External APIs  

If those slow down:
- Your agent must slow down too  

Otherwise:
- Threads pile up  
- Memory grows  
- System breaks  

---

## 8. Database Usage (Where Most Problems Happen)

### Use Connection Pooling

Never:
- Open a DB connection per event  

Always:
- Use a pool (e.g. HikariCP)

---

### Use Batch Inserts

Do NOT insert one row at a time.

Use batching:

```java
PreparedStatement stmt = connection.prepareStatement(SQL);

for (Event e : events) {
  stmt.setString(1, e.getId());
  stmt.setString(2, e.getPayload());
  stmt.addBatch();
}

stmt.executeBatch();
connection.commit();
```

**Why:**
- Much faster  
- Less load on DB  
- Fewer network round trips  

---

### Batch Size

Start with:
- 50–200 rows per batch  

Tune later.

---

### Keep Transactions Short

Do NOT:
- Hold DB connection while calling APIs  

---

## 9. Logging (Keep It Useful)

Use structured logs (JSON if possible).

Always include:
- Event ID  
- Status (success/failure)  
- Error message  
- Processing time  

Use proper log levels in the agent code. :

- `logger.debug()` - Log details relevant to debugging, like processing steps event payloads etc.
- `logger.info()`  - Keep these messages to a minimum - maybe a few lines during startup detailing the configuration, and a single info line per request.
- `logger.warn()`  - Warn on issues like JDBC reconnections 
- `logger.error()`  - Log all errors. Make sure to include any exceptions  

---

## 10. Error Handling

For each event:
- Try to process  
- Log result  
- Do not fail silently  

Avoid:
- Infinite retry loops  
- Blocking threads  

---

## 11. Scaling Reality Check

Agents scale — but not infinitely.

For high volume:
- Consider queues (Kafka, etc.)  
- Consider batch processing  

---

## 12. Startup & Shutdown

Startup:
- Validate config  
- Fail fast if broken  

Shutdown:
- Stop receiving events  
- Finish in-flight work  
- Close connections cleanly  

---

## 13. Common Mistakes

- Not acknowledging events quickly  
- Assuming events only arrive once  
- Writing to DB per event (no batching)  
- Too many threads  
- No limits on downstream calls  

---

## 14. Pre-Go-Live Checklist

- Events are acknowledged immediately  
- Duplicate handling works  
- DB batching is in place  
- Throughput tested  
- Logs are readable and useful  

---

## 15. Bottom Line

If you remember only this:

- Acknowledge fast  
- Expect duplicates  
- Control your threads  
- Batch your DB writes  

That gets you 80% of the way to a stable agent.
