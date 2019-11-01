# Reparent

```sql
SELECT root_record_id, parent_id, LEFT(title, 1) as initial, count(*) as objects
FROM archival_object
WHERE root_record_id = 8879 AND parent_id = 788927
GROUP BY initial
HAVING objects > 100
ORDER BY objects DESC
```
