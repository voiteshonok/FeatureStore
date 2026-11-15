-- Single write path for one transaction event (ADR-003).
-- KEYS: card zset, merchant minute hash, partition watermark, geo point
-- ARGV: offset, event_time_ms, member, now_ms, trim_ms, amount, declined,
--       ttl_seconds, ingest_time_ms, has_geo, geo
-- Returns "dup" when offset is at or below the partition watermark, else "applied".

local offset = tonumber(ARGV[1])
local prev = redis.call('HGET', KEYS[3], 'offset')
if prev and tonumber(prev) >= offset then
  return 'dup'
end

redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
local cutoff = tonumber(ARGV[4]) - tonumber(ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. string.format('%d', cutoff))

local fresh = redis.call('EXISTS', KEYS[2]) == 0
redis.call('HINCRBY', KEYS[2], 'count', 1)
redis.call('HINCRBY', KEYS[2], 'sum', ARGV[6])
redis.call('HINCRBY', KEYS[2], 'declined', ARGV[7])
if fresh then
  redis.call('EXPIRE', KEYS[2], ARGV[8])
end

if ARGV[10] == '1' then
  redis.call('SET', KEYS[4], ARGV[11])
end

redis.call('HSET', KEYS[3], 'offset', ARGV[1], 'as_of_ms', ARGV[9])
return 'applied'
