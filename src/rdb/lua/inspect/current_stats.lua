-- KEYS[1] ->  asynq:<qname>:pending
-- KEYS[2] ->  asynq:<qname>:active
-- KEYS[3] ->  asynq:<qname>:scheduled
-- KEYS[4] ->  asynq:<qname>:retry
-- KEYS[5] ->  asynq:<qname>:archived
-- KEYS[6] ->  asynq:<qname>:completed
-- KEYS[7] ->  asynq:<qname>:processed:<yyyy-mm-dd>
-- KEYS[8] ->  asynq:<qname>:failed:<yyyy-mm-dd>
-- KEYS[9] ->  asynq:<qname>:processed
-- KEYS[10] -> asynq:<qname>:failed
-- KEYS[11] -> asynq:<qname>:paused
-- KEYS[12] -> asynq:<qname>:groups
-- --------
-- ARGV[1] -> task key prefix
-- ARGV[2] -> group key prefix
local res = {}
local pendingTaskCount = redis.call("LLEN", KEYS[1])
table.insert(res, KEYS[1])
table.insert(res, pendingTaskCount)
table.insert(res, KEYS[2])
table.insert(res, redis.call("LLEN", KEYS[2]))
table.insert(res, KEYS[3])
table.insert(res, redis.call("ZCARD", KEYS[3]))
table.insert(res, KEYS[4])
table.insert(res, redis.call("ZCARD", KEYS[4]))
table.insert(res, KEYS[5])
table.insert(res, redis.call("ZCARD", KEYS[5]))
table.insert(res, KEYS[6])
table.insert(res, redis.call("ZCARD", KEYS[6]))
for i=7,10 do
    local count = 0
	local n = redis.call("GET", KEYS[i])
	if n then
	    count = tonumber(n)
	end
	table.insert(res, KEYS[i])
	table.insert(res, count)
end
table.insert(res, KEYS[11])
table.insert(res, redis.call("EXISTS", KEYS[11]))
table.insert(res, "oldest_pending_since")
if pendingTaskCount > 0 then
	local id = redis.call("LRANGE", KEYS[1], -1, -1)[1]
	table.insert(res, redis.call("HGET", ARGV[1] .. id, "pending_since"))
else
	table.insert(res, 0)
end
local group_names = redis.call("SMEMBERS", KEYS[12])
table.insert(res, "group_size")
table.insert(res, table.getn(group_names))
local aggregating_count = 0
for _, gname in ipairs(group_names) do
	aggregating_count = aggregating_count + redis.call("ZCARD", ARGV[2] .. gname)
end
table.insert(res, "aggregating_count")
table.insert(res, aggregating_count)
return res
