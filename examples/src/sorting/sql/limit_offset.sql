-- LIMIT and OFFSET for pagination.
-- LIMIT and OFFSET parameters are forced to IntType.
-- Generated function: paginate_posts(db:, limit: Int, offset: Int) -> Result(...)
SELECT
    id,
    title,
    published_at
FROM posts
ORDER BY published_at DESC
LIMIT
    @limit
    OFFSET @offset
