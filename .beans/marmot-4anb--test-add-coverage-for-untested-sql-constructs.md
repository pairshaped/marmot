---
# marmot-4anb
title: 'Test: Add coverage for untested SQL constructs'
status: todo
type: task
priority: normal
created_at: 2026-04-28T21:32:40Z
updated_at: 2026-04-28T21:32:40Z
---

Critical untested SQL constructs:

1. UPSERT / ON CONFLICT -- Extremely common. Parser may get confused by SET clause after ON CONFLICT (token stream differs from plain INSERT).
2. UNION / INTERSECT / EXCEPT -- Standard compound queries. Parser has zero awareness of these keywords; token boundaries in select-item and FROM parsing may break.
3. GROUP BY -- Parser recognizes GROUP as a boundary but untested. Interacts with aggregate function type inference.
4. SELECT DISTINCT -- Parser explicitly skips DISTINCT after SELECT, so it's supported, but zero coverage exists.
