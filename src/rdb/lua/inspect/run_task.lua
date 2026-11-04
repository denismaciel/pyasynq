-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- KEYS[2] -> asynq:{<qname>}:pending
-- KEYS[3] -> asynq:{<qname>}:groups
-- --
-- ARGV[1] -> task ID
-- ARGV[2] -> queue key prefix; asynq:{<qname>}:
-- ARGV[3] -> group key prefix
-- Output:
-- Returns 1 if task is successfully updated.
-- Returns 0 if task is not found.
-- Returns -1 if task is in active state.
-- Returns -2 if task is in pending state.
-- Returns error reply if unexpected error occurs.
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
	return -1
elseif state == "pending" then
	return -2
elseif state == "aggregating" then
	local n = redis.call("ZREM", ARGV[3] .. group, ARGV[1])
	if n == 0 then
		return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[3] .. group))
	end
	if redis.call("ZCARD", ARGV[3] .. group) == 0 then
		redis.call("SREM", KEYS[3], group)
	end
else
	local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
	if n == 0 then
		return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[2] .. state))
	end
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
