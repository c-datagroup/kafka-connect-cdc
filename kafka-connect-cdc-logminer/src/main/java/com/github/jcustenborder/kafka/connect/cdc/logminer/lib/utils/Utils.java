/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.logminer.lib.utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhengwx on 2017/6/9.
 */
public final class Utils {
    private static final Map<String, String[]> TEMPLATES = new ConcurrentHashMap();

    static String[] prepareTemplate(String template) {
        ArrayList list = new ArrayList();
        int pos = 0;

        for(int nextToken = template.indexOf("{}", pos); nextToken > -1 && pos < template.length(); nextToken = template.indexOf("{}", pos)) {
            list.add(template.substring(pos, nextToken));
            pos = nextToken + "{}".length();
        }

        list.add(template.substring(pos));
        return (String[])list.toArray(new String[list.size()]);
    }

    public static String format(String template, Object... args) {
        String[] templateArr = (String[])TEMPLATES.get(template);
        if(templateArr == null) {
            templateArr = prepareTemplate(template);
            TEMPLATES.put(template, templateArr);
        }

        StringBuilder sb = new StringBuilder(template.length() * 2);

        for(int i = 0; i < templateArr.length; ++i) {
            sb.append(templateArr[i]);
            if(args != null && i < templateArr.length - 1) {
                sb.append(i < args.length?args[i]:"{}");
            }
        }

        return sb.toString();
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling method is not null.
     *
     * @param value an object reference
     * @param varName the variable name to use in an exception message if the check fails
     * @return the non-null reference that was validated
     * @throws NullPointerException if {@code value} is null
     */
    public static <T> T checkNotNull(T value, Object varName) {
        if (value == null) {
            throw new NullPointerException(format("{} cannot be null", varName));
        }
        return value;
    }
}
