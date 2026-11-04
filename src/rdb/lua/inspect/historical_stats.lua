local res = {}
for _, key in ipairs(KEYS) do
	local n = redis.call("GET", key)
	if not n then
		n = 0
	end
	table.insert(res, tonumber(n))
end
return res
