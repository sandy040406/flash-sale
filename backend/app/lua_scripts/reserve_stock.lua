--[[
  Atomic Inventory Reservation Lua Script for Redis
  Ensures zero overselling under arbitrary concurrency.
  
  KEYS[1]: stock_key (e.g., "product:1:stock")
  KEYS[2]: user_reservations_set (e.g., "product:1:users")
  ARGV[1]: user_id
  ARGV[2]: requested_quantity (usually 1)

  Returns:
    1  -> Success: Stock reserved atomically
    0  -> Failure: Out of stock (Sold out)
   -1  -> Failure: User already placed an order (One per customer policy)
]]

local stock_key = KEYS[1]
local users_key = KEYS[2]
local user_id = ARGV[1]
local quantity = tonumber(ARGV[2]) or 1

-- 1. Check idempotency: Has this user already claimed an item?
if redis.call("SISMEMBER", users_key, user_id) == 1 then
    return -1
end

-- 2. Check current stock level
local current_stock = tonumber(redis.call("GET", stock_key))

if not current_stock or current_stock < quantity then
    return 0
end

-- 3. Atomically decrement stock and record user reservation
redis.call("DECRBY", stock_key, quantity)
redis.call("SADD", users_key, user_id)

return 1
