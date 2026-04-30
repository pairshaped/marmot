-- INSERT OR IGNORE: insert a row, or silently skip if it conflicts.
INSERT OR IGNORE INTO post_tags (post_id, tag_id)
VALUES (@post_id, @tag_id)
