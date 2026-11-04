-- KEYS[1] -> asynq:sema:<scope>
-- ARGV[1] -> max tokens
-- ARGV[2] -> current time in unix time
-- ARGV[3] -> deadline of the task (token expiry) in unix time
-- ARGV[4] -> task id
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", tonumber(ARGV[2])-1)
local count = redis.call("ZCARD", KEYS[1])

if (count < tonumber(ARGV[1])) then
     redis.call("ZADD", KEYS[1], ARGV[3], ARGV[4])
     return 'true'
else
     return 'false'
end
