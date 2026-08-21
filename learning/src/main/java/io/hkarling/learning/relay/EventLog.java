package io.hkarling.learning.relay;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class EventLog {

  private final List<String> log = new CopyOnWriteArrayList<>(); // 삭제되지 않는 append-only 로그

  void append(String event) {
    log.add(event);
  }

  // 컨슈머는 자기 오프셋을 스스로 들고 다니면서 조회
  String read(int offset) {
    return offset < log.size() ? log.get(offset) : null;
  }

  int size() {
    return log.size();
  }
}
