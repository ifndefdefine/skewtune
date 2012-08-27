/**
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
package skewtune.mapreduce.server.tasktracker;

import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Place holder for TaskTracker server-level configuration.
 * 
 * The keys should have "skewtune.tasktracker." as the prefix
 */
public interface TTConfig extends MRConfig {
    // Task-tracker configuration properties
    public static final String TT_HTTP_ADDRESS = 
        "skewtune.tasktracker.http.address";

    public static final String TT_HOST_NAME =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_HOST_NAME;

    public static final String TT_MAP_SLOTS = 
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_MAP_SLOTS;

    public static final String TT_REDUCE_SLOTS = 
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_REDUCE_SLOTS;

    public static final String TT_OUTOFBAND_HEARTBEAT =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_OUTOFBAND_HEARBEAT;

    public static final String TT_USER_NAME =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_USER_NAME;

    public static final String TT_KEYTAB_FILE =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_KEYTAB_FILE;

    public static final String TT_GROUP =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_GROUP;

    public static final String TT_DNS_INTERFACE =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_DNS_INTERFACE;

    public static final String TT_DNS_NAMESERVER =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_DNS_NAMESERVER;

    public static final String TT_REPORT_ADDRESS =
        "skewtune.tasktracker.report.address";
    
    public static final String TT_CONTENTION_TRACKING =
        org.apache.hadoop.mapreduce.server.tasktracker.TTConfig.TT_CONTENTION_TRACKING;
}
