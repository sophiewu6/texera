package no_config;

import java.io.Serializable;

public class Message {

  public interface abstr extends Serializable {
  }
  static class Greeting implements abstr {
    private static final long serialVersionUID = 1L;

    private final String text;

    public Greeting(String text) {
      this.text = text;
    }

    public String getText() {
      return text;
    }
  }
}