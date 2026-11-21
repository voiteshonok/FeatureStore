-- Single write path for one transaction event (ADR-003, ADR-016, ADR-017).
-- KEYS: card zset, merchant features hash (mfeat), partition watermark, geo point
-- ARGV: offset, event_time_ms, member, now_ms, trim_ms, amount, declined,
--       minute_expire_at_ms, hour_expire_at_ms, key_expire_at_ms,
--       ingest_time_ms, has_geo, geo
-- Returns "dup" when offset is at or below the partition watermark,
-- "dup_event" when the card member was already present (ZADD added 0),
-- else "applied".

local offset = tonumber(ARGV[1])
local prev = redis.call('HGET', KEYS[3], 'offset')
if prev and tonumber(prev) >= offset then
  return 'dup'
end

local added = redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
if added == 0 then
  redis.call('HSET', KEYS[3], 'offset', ARGV[1], 'as_of_ms', ARGV[11])
  return 'dup_event'
end

local cutoff = tonumber(ARGV[4]) - tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. string.format('%d', cutoff))

local event_time_ms = tonumber(ARGV[2])
local minute = math.floor(event_time_ms / 60000)
local hour = math.floor(minute / 60)
local m_c = 'm:' .. minute .. ':c'
local m_s = 'm:' .. minute .. ':s'
local m_d = 'm:' .. minute .. ':d'
local h_c = 'h:' .. hour .. ':c'
local h_s = 'h:' .. hour .. ':s'
local h_d = 'h:' .. hour .. ':d'
redis.call('HINCRBY', KEYS[2], m_c, 1)
redis.call('HINCRBY', KEYS[2], m_s, ARGV[6])
redis.call('HINCRBY', KEYS[2], m_d, ARGV[7])
redis.call('HINCRBY', KEYS[2], h_c, 1)
redis.call('HINCRBY', KEYS[2], h_s, ARGV[6])
redis.call('HINCRBY', KEYS[2], h_d, ARGV[7])
redis.call('HPEXPIREAT', KEYS[2], ARGV[8], 'FIELDS', 3, m_c, m_s, m_d)
redis.call('HPEXPIREAT', KEYS[2], ARGV[9], 'FIELDS', 3, h_c, h_s, h_d)

local function pexpire_at_gt(key, at)
  local ttl = redis.call('PTTL', key)
  if ttl < 0 then
    redis.call('PEXPIREAT', key, at)
  else
    redis.call('PEXPIREAT', key, at, 'GT')
  end
end

pexpire_at_gt(KEYS[1], ARGV[10])

if ARGV[12] == '1' then
  -- KEEPTTL so a late rewrite does not drop the previous expiry before GT.
  redis.call('SET', KEYS[4], ARGV[13], 'KEEPTTL')
  pexpire_at_gt(KEYS[4], ARGV[10])
end

redis.call('HSET', KEYS[3], 'offset', ARGV[1], 'as_of_ms', ARGV[11])
return 'applied'
