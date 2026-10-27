local types = {'zset', 'hash', 'hash'}
for i, expected in ipairs(types) do
  local actual = redis.call('TYPE', KEYS[i]).ok
  if actual ~= 'none' and actual ~= expected then
    return redis.error_reply('unexpected authorization key type')
  end
end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local first = tonumber(ARGV[3])
local horizon = tonumber(ARGV[4])
if now >= first + horizon or first > now + 5000 then
  return {'expired_retry', '', 0, now, 0, 0}
end
local existing = redis.call('HMGET', KEYS[3], 'digest', 'registered')
if existing[1] and existing[1] ~= ARGV[2] then
  return {'conflict', '', 0, now, 0, 0}
end
local run = string.match(redis.call('INFO', 'server'), 'run_id:([^\r\n]+)')
if not run then return redis.error_reply('missing Redis process identity') end
local epoch = ARGV[5] .. ':' .. run
local state = redis.call('HMGET', KEYS[2], 'epoch', 'since', 'last')
local since = tonumber(state[2])
if state[1] ~= epoch or not since or now < tonumber(state[3]) then
  since = now
end
redis.call('HSET', KEYS[2], 'epoch', epoch, 'since', since, 'last', now)
redis.call('PEXPIRE', KEYS[2], horizon + 60000)
local registered = tonumber(existing[2])
if not registered then
  registered = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1])) or now
  redis.call('ZADD', KEYS[1], registered, ARGV[1])
  redis.call('HSET', KEYS[3], 'digest', ARGV[2], 'registered', registered)
  redis.call('PEXPIRE', KEYS[3], horizon + 60000)
end
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now - 60000, 'LIMIT', 0, 256)
if #expired > 0 then redis.call('ZREM', KEYS[1], unpack(expired)) end
redis.call('PEXPIRE', KEYS[1], horizon + 60000)
local short = redis.call('ZCOUNT', KEYS[1], '(' .. (now - 10000), now)
local long = redis.call('ZCOUNT', KEYS[1], '(' .. (now - 60000), now)
local status = 'coverage_unknown'
if now - since >= 60000 then status = 'registered' end
return {status, epoch, registered, now, short, long}
