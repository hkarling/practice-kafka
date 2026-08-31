package io.hkarling.learning.idempotency;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class IdempotentOrderLogRepository {

  private final JdbcTemplate jdbcTemplate;

  /**
   * true = 처음 처리(새로 기록됨), false = 이미 처리된 이벤트(중복)
   */
  public boolean tryMarkProcessed(String orderId) {
    int updated = jdbcTemplate.update(
        "INSERT INTO idempotent_order_log (order_id) VALUES (?) ON CONFLICT (order_id) DO NOTHING",
        orderId);
    return updated == 1;
  }

  public int countByOrderId(String orderId) {
    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM idempotent_order_log WHERE order_id = ?", Integer.class, orderId);
    return count == null ? 0 : count;
  }
  
}
