/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * This source code is available under agreement available at
 * %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
 *
 * You should have received a copy of the agreement
 * along with this program; if not, write to Talend SA
 * 9 rue Pages 92150 Suresnes, France
 */
package org.talend.components.processing.python3;

import org.talend.daikon.exception.ExceptionContext;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.ErrorCode;

import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.Collection;

public enum Python3ErrorCode implements ErrorCode {

    INVALID_PYTHON_IMPORT_EXCEPTION(
            "INVALID_PYTHON_IMPORT_EXCEPTION",
            HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            "importName");

    private final String code;

    private final int httpStatus;

    private final Collection<String> contextEntries;

    Python3ErrorCode(String code, int httpStatus, String... contextEntries) {
        this.httpStatus = httpStatus;
        this.code = code;
        this.contextEntries = Arrays.asList(contextEntries);
    }

    @Override
    public String getProduct() {
        return "Talend";
    }

    @Override
    public String getGroup() {
        return "Processing";
    }

    @Override
    public int getHttpStatus() {
        return httpStatus;
    }

    @Override
    public Collection<String> getExpectedContextEntries() {
        return contextEntries;
    }

    @Override
    public String getCode() {
        return code;
    }

    /**
     * Create an exception when the PythonRow is trying to import something that can make a security issue.
     *
     * @param importName The name of the invalid import
     * @return An exception corresponding to the error code.
     */
    public static TalendRuntimeException createInvalidPythonImportErrorException(String importName) {
        return new TalendMsgRuntimeException(INVALID_PYTHON_IMPORT_EXCEPTION,
                ExceptionContext.withBuilder().put("importName", importName).build(),
                "The import '" + importName + "' cannot be used.");
    }

    /**
     * {@link TalendRuntimeException} with a reasonable user-friendly message in English.
     */
    private static class TalendMsgRuntimeException extends TalendRuntimeException {

        private final String localizedMessage;

        public TalendMsgRuntimeException(Throwable cause, ErrorCode code, ExceptionContext context, String localizedMessage) {
            super(code, cause, context);
            this.localizedMessage = localizedMessage;
        }

        public TalendMsgRuntimeException(ErrorCode code, ExceptionContext context, String localizedMessage) {
            super(code, context);
            this.localizedMessage = localizedMessage;
        }

        @Override
        public String getMessage() {
            return getLocalizedMessage();
        }

        @Override
        public String getLocalizedMessage() {
            return localizedMessage;
        }
    }
}
