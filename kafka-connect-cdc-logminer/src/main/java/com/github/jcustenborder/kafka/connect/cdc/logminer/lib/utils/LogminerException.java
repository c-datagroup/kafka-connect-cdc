package com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils;

/**
 * Created by zhengwx on 2017/6/9.
 */
public class LogminerException extends Exception {
    private final transient ErrorCode errorCode;
    private final transient ErrorMessage errorMessage;
    private final transient Object[] params;

    /**
     * Exception constructor.
     *
     * @param errorCode error code.
     * @param params parameters for the error code message template, if the last configName is an exception it is
     * considered the cause of the exception.
     */
    public LogminerException(ErrorCode errorCode, Object... params) {
        this.errorCode = errorCode;
        this.params = params;
        errorMessage = new ErrorMessage(errorCode, params);
        Throwable cause = getCause(params);
        if(cause != null) {
            initCause(cause);
        }
    }

    private static Throwable getCause(Object... params) {
        Throwable throwable = null;
        if (params.length > 0 && params[params.length - 1] instanceof Throwable) {
            throwable = (Throwable) params[params.length - 1];
        }
        return throwable;
    }

    /**
     * Returns the error code associated with the exception.
     *
     * @return the error code associated with the exception.
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the error parameters of the exception.
     *
     * @return the error parameters of the exception.
     */
    public Object[] getParams() {
        return params;
    }

    /**
     * Returns the exception message, not localized.
     *
     * @return the exception message, not localized.
     */
    @Override
    public String getMessage() {
        return errorMessage.getNonLocalized();
    }

    /**
     * Returns the exception message, localized.
     *
     * @return the exception message, localized.
     */
    @Override
    public String getLocalizedMessage() {
        return errorMessage.getLocalized();
    }
}
