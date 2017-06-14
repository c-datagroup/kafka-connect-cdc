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

import java.util.Locale;

/**
 * Created by zhengwx on 2017/6/9.
 */
public abstract class LocaleInContext {
    private static final ThreadLocal<Locale> LOCALE_TL = new ThreadLocal<Locale>() {
        @Override
        protected Locale initialValue() {
            return Locale.getDefault();
        }
    };

    private LocaleInContext() {}

    public static void set(Locale locale) {
        LOCALE_TL.set((locale != null) ? locale : Locale.getDefault());
    }

    public static Locale get() {
        return LOCALE_TL.get();
    }

}
