-- KEYS[1] -> asynq:{qname}:active
-- KEYS[2] -> asynq:{qname}:pending
-- KEYS[3] -> asynq:{qname}:scheduled
-- KEYS[4] -> asynq:{qname}:retry
-- KEYS[5] -> asynq:{qname}:archived
-- KEYS[6] -> asynq:{qname}:completed
-- KEYS[7] -> asynq:{qname}:groups
-- -------
-- ARGV[1] -> asynq:{qname}:t: (task key prefix)
-- ARGV[2] -> task sample size per redis list/zset (e.g 20)
-- ARGV[3] -> group sample size
-- ARGV[4] -> asynq:{qname}:g: (group key prefix)
local sample_size = tonumber(ARGV[2])
if sample_size <= 0 then
    return redis.error_reply("sample size must be a positive number")
end
local memusg = 0
for i=1,2 do
    local ids = redis.call("LRANGE", KEYS[i], 0, sample_size - 1)
    local sample_total = 0
    if (table.getn(ids) > 0) then
        for _, id in ipairs(ids) do
            local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
            if bytes then
                sample_total = sample_total + bytes
            end
        end
        local n = redis.call("LLEN", KEYS[i])
        local avg = sample_total / table.getn(ids)
        memusg = memusg + (avg * n)
    end
    local m = redis.call("MEMORY", "USAGE", KEYS[i])
    if (m) then
        memusg = memusg + m
    end
end
for i=3,6 do
    local ids = redis.call("ZRANGE", KEYS[i], 0, sample_size - 1)
    local sample_total = 0
    if (table.getn(ids) > 0) then
        for _, id in ipairs(ids) do
            local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
            if bytes then
                sample_total = sample_total + bytes
            end
        end
        local n = redis.call("ZCARD", KEYS[i])
        local avg = sample_total / table.getn(ids)
        memusg = memusg + (avg * n)
    end
    local m = redis.call("MEMORY", "USAGE", KEYS[i])
    if (m) then
        memusg = memusg + m
    end
end
local groups = redis.call("SMEMBERS", KEYS[7])
if table.getn(groups) > 0 then
	local agg_task_count = 0
	local agg_task_sample_total = 0
	local agg_task_sample_size = 0
	for i, gname in ipairs(groups) do
		local group_key = ARGV[4] .. gname
		agg_task_count = agg_task_count + redis.call("ZCARD", group_key)
		if i <= tonumber(ARGV[3]) then
			local ids = redis.call("ZRANGE", group_key, 0, sample_size - 1)
			for _, id in ipairs(ids) do
				local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
				if bytes then
					agg_task_sample_total = agg_task_sample_total + bytes
					agg_task_sample_size = agg_task_sample_size + 1
				end
			end
		end
	end
	if agg_task_sample_size > 0 then
		local avg = agg_task_sample_total / agg_task_sample_size
		memusg = memusg + (avg * agg_task_count)
	end
end
return memusg
