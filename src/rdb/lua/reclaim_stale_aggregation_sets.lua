-- KEYS[1] -> asynq:{<qname>}:aggregation_sets
-- KEYS[2] -> asynq:{<qname>}:groups
-- -------
-- ARGV[1] -> current time in unix time
-- ARGV[2] -> group key prefix (asynq:{<qname>}:g:)
--
-- Puts the tasks of stale aggregation sets back into their groups and
-- re-registers the groups, so the aggregator finds them again.
local staleSetKeys = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, key in ipairs(staleSetKeys) do
	local idx = string.find(key, ":[^:]*$")
	local groupKey = string.sub(key, 1, idx-1)
	local res = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", groupKey, tonumber(res[i+1]), res[i])
	end
	if table.getn(res) > 0 then
		redis.call("SADD", KEYS[2], string.sub(groupKey, string.len(ARGV[2]) + 1))
	end
	redis.call("DEL", key)
end
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
return redis.status_reply("OK")
