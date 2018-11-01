package edu.uci.ics.texera.api.exception;

public class LineageException extends TexeraException {

    private static final long serialVersionUID = -7393624288798221759L;
    
    public LineageException(String errorMessage) {
        super(errorMessage);
    }

    public LineageException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public LineageException(Throwable throwable) {
        super(throwable);
    }
    
}
