-- KEYS[1] -> asynq:{<qname>}:pending
-- KEYS[2] -> asynq:{<qname>}:active
-- KEYS[3] -> asynq:{<qname>}:scheduled
-- KEYS[4] -> asynq:{<qname>}:retry
-- KEYS[5] -> asynq:{<qname>}:archived
-- KEYS[6] -> asynq:{<qname>}:lease
-- KEYS[7] -> asynq:{<qname>}:completed
-- KEYS[8] -> asynq:{<qname>}:groups
-- KEYS[9] -> asynq:{<qname>}:aggregation_sets
-- --
-- ARGV[1] -> task key prefix
-- ARGV[2] -> group key prefix
-- Output:
-- Returns 1 if successfully removed.
-- Returns -2 if the queue has active tasks.
if redis.call("LLEN", KEYS[2]) > 0 then
	return -2
end
local ids = {}
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do table.insert(ids, id) end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do table.insert(ids, id) end
for i = 3, 5 do
	for _, id in ipairs(redis.call("ZRANGE", KEYS[i], 0, -1)) do table.insert(ids, id) end
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[7], 0, -1)) do table.insert(ids, id) end
local setKeys = {}
for _, group in ipairs(redis.call("SMEMBERS", KEYS[8])) do
	table.insert(setKeys, ARGV[2] .. group)
end
for _, key in ipairs(redis.call("ZRANGE", KEYS[9], 0, -1)) do
	table.insert(setKeys, key)
end
for _, key in ipairs(setKeys) do
	for _, id in ipairs(redis.call("ZRANGE", key, 0, -1)) do table.insert(ids, id) end
end
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, key in ipairs(setKeys) do
	redis.call("DEL", key)
end
for i = 1, 9 do
	redis.call("DEL", KEYS[i])
end
return 1
