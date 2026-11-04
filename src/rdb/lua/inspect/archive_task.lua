-- KEYS[1] -> task key (asynq:{<qname>}:t:<task_id>)
-- KEYS[2] -> archived key (asynq:{<qname>}:archived)
-- KEYS[3] -> all groups key (asynq:{<qname>}:groups)
-- --
-- ARGV[1] -> id of the task to archive
-- ARGV[2] -> current timestamp
-- ARGV[3] -> cutoff timestamp (e.g., 90 days ago)
-- ARGV[4] -> max number of tasks in archived state (e.g., 100)
-- ARGV[5] -> queue key prefix (asynq:{<qname>}:)
-- ARGV[6] -> group key prefix (asynq:{<qname>}:g:)
-- Output:
-- Returns 1 if task is successfully archived.
-- Returns 0 if task is not found.
-- Returns -1 if task is already archived.
-- Returns -2 if task is in active state.
-- Returns error reply if unexpected error occurs.
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
	return -2
end
if state == "archived" then
	return -1
end
if state == "pending" then
	if redis.call("LREM", ARGV[5] .. state, 1, ARGV[1]) == 0 then
		return redis.error_reply("task id not found in list " .. tostring(ARGV[5] .. state))
	end
elseif state == "aggregating" then
	if redis.call("ZREM", ARGV[6] .. group, ARGV[1]) == 0 then
		return redis.error_reply("task id not found in zset " .. tostring(ARGV[6] .. group))
	end
	if redis.call("ZCARD", ARGV[6] .. group) == 0 then
		redis.call("SREM", KEYS[3], group)
	end
else
	if redis.call("ZREM", ARGV[5] .. state, ARGV[1]) == 0 then
		return redis.error_reply("task id not found in zset " .. tostring(ARGV[5] .. state))
	end
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "archived")
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[4])
return 1
