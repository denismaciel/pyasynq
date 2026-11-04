-- KEYS[1] -> asynq:{<qname>}:active
-- KEYS[2] -> asynq:{<qname>}:lease
-- KEYS[3] -> asynq:{<qname>}:pending
-- KEYS[4] -> asynq:{<qname>}:t:<task_id>
-- ARGV[1] -> task ID
-- RPUSH puts the task at the head of the queue (dequeue pops from the right).
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")
