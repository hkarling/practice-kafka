package io.hkarling.learning.outbox;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class OutboxEventRepository {

  private final JdbcTemplate jdbcTemplate;

  public void save(String aggregateId, String topic, String payload) {
    jdbcTemplate.update("INSERT INTO outbox_event (aggregate_id, topic, payload) VALUES (?, ?, ?)",
        aggregateId, topic, payload);
  }

  public int countByAggregateId(String aggregateId) {
    Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ?",
        Integer.class, aggregateId);
    return count == null ? 0 : count;
  }

  public List<OutboxEventRow> findPending(int limit) {
    return jdbcTemplate.query(
        "SELECT id, topic, aggregate_id, payload FROM outbox_event WHERE status = 'PENDING' ORDER BY id LIMIT ?",
        (rs, rowNum) -> new OutboxEventRow(
            rs.getLong("id"),
            rs.getString("topic"),
            rs.getString("aggregate_id"),
            rs.getString("payload")),
        limit);
  }

  public void markPublished(Long id) {
    jdbcTemplate.update(
        "UPDATE outbox_event SET status = 'PUBLISHED', published_at = now() WHERE id = ?", id);
  }

}
