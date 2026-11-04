-- KEYS[1] -> ZSET to move task from (e.g., asynq:{<qname>}:retry)
-- KEYS[2] -> asynq:{<qname>}:archived
-- --
-- ARGV[1] -> current timestamp
-- ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
-- ARGV[3] -> max number of tasks in archive (e.g., 100)
-- ARGV[4] -> task key prefix (asynq:{<qname>}:t:)
-- Output:
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("ZADD", KEYS[2], ARGV[1], id)
	redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
return table.getn(ids)
