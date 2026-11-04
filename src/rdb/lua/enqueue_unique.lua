-- KEYS[1] -> unique key
-- KEYS[2] -> asynq:{<qname>}:t:<task_id>
-- KEYS[3] -> asynq:{<qname>}:pending
-- ARGV[1] -> task ID
-- ARGV[2] -> uniqueness lock TTL in seconds
-- ARGV[3] -> task message data
-- ARGV[4] -> current unix time in nsec
--
-- Returns 1 if enqueued, 0 if the task ID already exists,
-- -1 if the unique lock is already held.
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
