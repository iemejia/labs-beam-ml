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

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Documentation("Python configuration.")
@OptionsOrder({ "pythonCode" })
public class Python3Configuration implements Serializable {

    private static StringBuilder defaultPython = new StringBuilder()
            .append("# Here you can define your custom MAP transformations on the input\n")
            .append("# The input record is available as the \"input\" variable\n")
            .append("# The output record is available as the \"output\" variable\n")
            .append("# The record columns are available as defined in your input/output schema\n")
            .append("# The return statement is added automatically to the generated code,\n")
            .append("# so there's no need to add it here\n\n").append("# Code Sample :\n\n")
            .append("# 1. When choosing Map, output is a dictionary\n\n").append("# output['col1'] = input['col1'] + 1234\n")
            .append("# output['col2'] = \"The \" + input['col2'] + \":\"\n")
            .append("# output['col3'] = CustomTransformation(input['col3'])\n\n\n")
            .append("# ---------------------------------------------------------------\n\n")
            .append("# 2. When choosing FlatMap, output is a list of dictionaries\n\n").append("# recordOne = input\n")
            .append("# recordOne['col1'] = 'newOne'\n").append("# output.append(recordOne)\n");

    @Option
    @Required
    @Code("python")
    @Documentation("The Python code")
    private String pythonCode = defaultPython.toString();

    public void setPythonCode(String pythonCode) {
        this.pythonCode = pythonCode;
    }
}
