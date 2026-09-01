package io.hkarling.learning.cqrs;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class OrderSummaryRepository {

  private final JdbcTemplate jdbcTemplate;

  public void upsert(String orderId, String status) {
    jdbcTemplate.update("""
         INSERT INTO order_summary (order_id, status, updated_at)
         VALUES (?, ?, NOW())
         ON CONFLICT (order_id)
         DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = NOW()
        """, orderId, status);
  }

  public int countByOrderId(String orderId) {
    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM order_summary WHERE order_id = ?", Integer.class, orderId);
    return count == null ? 0 : count;
  }

}
