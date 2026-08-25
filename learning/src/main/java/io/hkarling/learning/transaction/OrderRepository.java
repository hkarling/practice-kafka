package io.hkarling.learning.transaction;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@RequiredArgsConstructor
@Repository
public class OrderRepository {

  private final JdbcTemplate jdbcTemplate;

  public void save(String orderId, String status) {
    jdbcTemplate.update(
        "INSERT INTO orders (order_id, status) VALUES (?, ?)", orderId, status);
  }

  public int countByOrderId(String orderId) {
    Integer count = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM orders WHERE order_id = ?", Integer.class, orderId);
    return count == null ? 0 : count;
  }

}
