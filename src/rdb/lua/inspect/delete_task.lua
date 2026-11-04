-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- KEYS[2] -> asynq:{<qname>}:groups
-- --
-- ARGV[1] -> task ID
-- ARGV[2] -> queue key prefix
-- ARGV[3] -> group key prefix
-- Output:
-- Returns 1 if task is successfully deleted.
-- Returns 0 if task is not found.
-- Returns -1 if task is in active state.
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
	return -1
end
if state == "pending" then
	if redis.call("LREM", ARGV[2] .. state, 0, ARGV[1]) == 0 then
		return redis.error_reply("task is not found in list: " .. tostring(ARGV[2] .. state))
	end
elseif state == "aggregating" then
	if redis.call("ZREM", ARGV[3] .. group, ARGV[1]) == 0 then
		return redis.error_reply("task is not found in zset: " .. tostring(ARGV[3] .. group))
	end
	if redis.call("ZCARD", ARGV[3] .. group) == 0 then
		redis.call("SREM", KEYS[2], group)
	end
else
	if redis.call("ZREM", ARGV[2] .. state, ARGV[1]) == 0 then
		return redis.error_reply("task is not found in zset: " .. tostring(ARGV[2] .. state))
	end
end
local unique_key = redis.call("HGET", KEYS[1], "unique_key")
if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == ARGV[1] then
	redis.call("DEL", unique_key)
end
return redis.call("DEL", KEYS[1])
