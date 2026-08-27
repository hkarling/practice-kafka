package io.hkarling.learning.semantics;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class OrderProcessingLogRepository {

  private final JdbcTemplate jdbcTemplate;

  public void insert(String orderId) {
    jdbcTemplate.update("INSERT INTO order_processing_log (order_id) VALUES (?)", orderId);
  }

  public int countByOrderId(String orderId) {
    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM order_processing_log WHERE order_id = ?", Integer.class, orderId);
    return count == null ? 0 : count;
  }

}
