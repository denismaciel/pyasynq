-- KEYS[1] -> asynq:{<qname>}
-- KEYS[2] -> asynq:{<qname>}:active
-- KEYS[3] -> asynq:{<qname>}:scheduled
-- KEYS[4] -> asynq:{<qname>}:retry
-- KEYS[5] -> asynq:{<qname>}:archived
-- KEYS[6] -> asynq:{<qname>}:lease
-- --
-- ARGV[1] -> task key prefix
-- Output:
-- Returns 1 if successfully removed.
-- Returns -2 if the queue has active tasks.
local active = redis.call("LLEN", KEYS[2])
if active > 0 then
    return -2
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 1
