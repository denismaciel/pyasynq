-- KEYS[1] -> asynq:{<qname>}:g:<gname>
-- KEYS[2] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
-- KEYS[3] -> asynq:{<qname>}:aggregation_sets
-- KEYS[4] -> asynq:{<qname>}:groups
-- -------
-- ARGV[1] -> max group size
-- ARGV[2] -> max group delay in unix time
-- ARGV[3] -> start time of the grace period
-- ARGV[4] -> aggregation set expire time
-- ARGV[5] -> current time in unix time
-- ARGV[6] -> group name
-- Output:
-- Returns 0 if no aggregation set was created
-- Returns 1 if an aggregation set was created
local size = redis.call("ZCARD", KEYS[1])
if size == 0 then
	return 0
end
local maxSize = tonumber(ARGV[1])
if maxSize ~= 0 and size >= maxSize then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size == maxSize then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
local maxDelay = tonumber(ARGV[2])
local currentTime = tonumber(ARGV[5])
if maxDelay ~= 0 then
	local oldestEntry = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
	local oldestEntryScore = tonumber(oldestEntry[2])
	local maxDelayTime = currentTime - maxDelay
	if oldestEntryScore <= maxDelayTime then
		local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
		for i=1, table.getn(res)-1, 2 do
			redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
		end
		redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
		redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
		if size <= maxSize or maxSize == 0 then
			redis.call("SREM", KEYS[4], ARGV[6])
		end
		return 1
	end
end
local latestEntry = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
local latestEntryScore = tonumber(latestEntry[2])
local gracePeriodStartTime = currentTime - tonumber(ARGV[3])
if latestEntryScore <= gracePeriodStartTime then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size <= maxSize or maxSize == 0 then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
return 0
