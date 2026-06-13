"""Server-side Lua scripts for the operations that must be atomic.

Scores are formatted with ``string.format('%.0f', ...)`` so large integer scores are
never handed to Redis in scientific notation. ``priority * 1e13 + seq`` stays below 2^53,
so it is represented exactly by a ZSET's double-precision score. ``seq`` is a global
monotonic counter (INCR) — unlike a millisecond timestamp it can never tie, so FIFO
within a priority holds even when many producers enqueue inside the same millisecond.
"""
from __future__ import annotations

# Atomically persist a task and place it on the ready queue with a tie-free FIFO score.
# KEYS[1]=ready  KEYS[2]=task_key  KEYS[3]=seq  KEYS[4]=recent
# ARGV[1]=task_id  ARGV[2]=priority  ARGV[3]=task_json
# Returns the sequence number assigned to the task.
ENQUEUE_READY = """
local seq = redis.call('INCR', KEYS[3])
redis.call('SET', KEYS[2], ARGV[3])
local score = string.format('%.0f', ARGV[2] * 1e13 + seq)
redis.call('ZADD', KEYS[1], score, ARGV[1])
redis.call('LPUSH', KEYS[4], ARGV[1])
redis.call('LTRIM', KEYS[4], 0, 999)
return seq
"""

# Atomically claim the highest-priority ready task and lease it to a worker.
# KEYS[1]=ready  KEYS[2]=inflight
# ARGV[1]=visibility_deadline_ms  ARGV[2]=task_key_prefix
# Returns the task JSON payload, or false if the queue is empty.
DEQUEUE = """
local popped = redis.call('ZPOPMIN', KEYS[1], 1)
if (not popped) or (#popped == 0) then
  return false
end
local task_id = popped[1]
local payload = redis.call('GET', ARGV[2] .. task_id)
if not payload then
  -- task record was cancelled/expired; drop it (already removed from ready)
  return false
end
redis.call('ZADD', KEYS[2], ARGV[1], task_id)
return payload
"""

# Move all due delayed tasks into the ready queue, re-deriving each task's priority score.
# KEYS[1]=delayed  KEYS[2]=ready  KEYS[3]=seq
# ARGV[1]=now_ms  ARGV[2]=limit  ARGV[3]=task_key_prefix
# Returns the number of tasks promoted.
PROMOTE = """
local now = ARGV[1]
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[2])
local moved = 0
for i = 1, #due do
  local task_id = due[i]
  local payload = redis.call('GET', ARGV[3] .. task_id)
  if payload then
    local prio = 100
    local ok, t = pcall(cjson.decode, payload)
    if ok and type(t) == 'table' and t.priority then
      prio = t.priority
    end
    local seq = redis.call('INCR', KEYS[3])
    local score = string.format('%.0f', prio * 1e13 + seq)
    redis.call('ZADD', KEYS[2], score, task_id)
  end
  redis.call('ZREM', KEYS[1], task_id)
  moved = moved + 1
end
return moved
"""

# Requeue in-flight tasks whose visibility deadline has passed (worker crash recovery).
# KEYS[1]=inflight  KEYS[2]=ready  KEYS[3]=seq
# ARGV[1]=now_ms  ARGV[2]=limit  ARGV[3]=task_key_prefix
# Returns the number of tasks reclaimed.
REAP = """
local now = ARGV[1]
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[2])
local n = 0
for i = 1, #expired do
  local task_id = expired[i]
  local payload = redis.call('GET', ARGV[3] .. task_id)
  if payload then
    local prio = 100
    local ok, t = pcall(cjson.decode, payload)
    if ok and type(t) == 'table' and t.priority then
      prio = t.priority
    end
    local seq = redis.call('INCR', KEYS[3])
    local score = string.format('%.0f', prio * 1e13 + seq)
    redis.call('ZADD', KEYS[2], score, task_id)
  end
  redis.call('ZREM', KEYS[1], task_id)
  n = n + 1
end
return n
"""

# Remove a task from the ready and/or delayed sets (cancellation).
# KEYS[1]=ready  KEYS[2]=delayed  ARGV[1]=task_id
# Returns how many sets it was removed from (0 means it was not cancellable).
CANCEL = """
local r = redis.call('ZREM', KEYS[1], ARGV[1])
local d = redis.call('ZREM', KEYS[2], ARGV[1])
return r + d
"""

# Extend the leader lock only if we still own it (compare-and-extend).
# KEYS[1]=lock  ARGV[1]=token  ARGV[2]=ttl_ms
REFRESH_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"""
