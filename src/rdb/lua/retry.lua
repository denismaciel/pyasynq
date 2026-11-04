-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- KEYS[2] -> asynq:{<qname>}:active
-- KEYS[3] -> asynq:{<qname>}:lease
-- KEYS[4] -> asynq:{<qname>}:retry
-- KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
-- KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
-- KEYS[7] -> asynq:{<qname>}:processed
-- KEYS[8] -> asynq:{<qname>}:failed
-- ARGV[1] -> task ID
-- ARGV[2] -> updated task message
-- ARGV[3] -> retry_at unix timestamp
-- ARGV[4] -> stats expiration timestamp
-- ARGV[5] -> is_failure (1 or 0)
-- ARGV[6] -> max int64 value
-- ARGV[7] -> lease cutoff unix timestamp, or -1; with a cutoff, only a task whose
--             lease expired by then is moved
if tonumber(ARGV[7]) >= 0 then
  local lease = redis.call("ZSCORE", KEYS[3], ARGV[1])
  if not lease or tonumber(lease) > tonumber(ARGV[7]) then
    return redis.error_reply("NOT FOUND")
  end
end
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
if tonumber(ARGV[5]) == 1 then
	local n = redis.call("INCR", KEYS[5])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[5], ARGV[4])
	end
	local m = redis.call("INCR", KEYS[6])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[6], ARGV[4])
	end
    local total = redis.call("GET", KEYS[7])
    if tonumber(total) == tonumber(ARGV[6]) then
    	redis.call("SET", KEYS[7], 1)
    	redis.call("SET", KEYS[8], 1)
    else
    	redis.call("INCR", KEYS[7])
    	redis.call("INCR", KEYS[8])
    end
end
return redis.status_reply("OK")
