-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- KEYS[2] -> asynq:{<qname>}:pending
-- ARGV[1] -> task message data
-- ARGV[2] -> task ID
-- ARGV[3] -> current unix time in nsec
--
-- Returns 1 if enqueued, 0 if the task ID already exists.
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
