-- LIMIT/OFFSET parameters are forced to IntType.
-- Generated function: top_users(db:, limit: Int) -> Result(...)
SELECT id, name, email
FROM users
ORDER BY created_at DESC
LIMIT
@limit
