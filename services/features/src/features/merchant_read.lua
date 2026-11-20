-- Sum one merchant features hash over [first, current] minutes (ADR-016).
-- KEYS[1] = mfeat:{merchant} hash with fields m:{min}:c|d and h:{hour}:c|d
-- ARGV[1] = first minute (inclusive), ARGV[2] = current minute (inclusive)
-- Returns {total_count, total_declined}

local first = tonumber(ARGV[1])
local current = tonumber(ARGV[2])
local total_count = 0
local total_declined = 0
local minute = first
while minute <= current do
  local hour = math.floor(minute / 60)
  local hour_start = hour * 60
  local hour_end = hour_start + 59
  if hour_start >= first and hour_end <= current then
    local count = tonumber(redis.call('HGET', KEYS[1], 'h:' .. hour .. ':c'))
    local declined = tonumber(redis.call('HGET', KEYS[1], 'h:' .. hour .. ':d'))
    if count then
      total_count = total_count + count
    end
    if declined then
      total_declined = total_declined + declined
    end
    minute = hour_end + 1
  else
    local count = tonumber(redis.call('HGET', KEYS[1], 'm:' .. minute .. ':c'))
    local declined = tonumber(redis.call('HGET', KEYS[1], 'm:' .. minute .. ':d'))
    if count then
      total_count = total_count + count
    end
    if declined then
      total_declined = total_declined + declined
    end
    minute = minute + 1
  end
end
return {total_count, total_declined}
