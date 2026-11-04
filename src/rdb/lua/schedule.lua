-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- KEYS[2] -> asynq:{<qname>}:scheduled
-- ARGV[1] -> task message data
-- ARGV[2] -> process_at time in unix seconds
-- ARGV[3] -> task ID
-- ARGV[4] -> group key, or "" (forwarded into the group when due)
--
-- Returns 1 if scheduled, 0 if the task ID already exists.
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
if ARGV[4] ~= "" then
  redis.call("HSET", KEYS[1], "group", ARGV[4])
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
