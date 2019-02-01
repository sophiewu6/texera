package my.cluster;

import java.io.Serializable;

public interface Messages {

  public static class TextJob implements Serializable {
    private final String text;

    public TextJob(String text) {
      this.text = text;
    }

    public String getText() {
      return text;
    }
  }

  public static class JobFailed implements Serializable {
    private final String reason;

    public JobFailed(String reason) {
      this.reason = reason;
    }

    public String getReason() {
      return reason;
    }

    @Override
    public String toString() {
      return "JobFailed(" + reason + ")";
    }
  }

}
