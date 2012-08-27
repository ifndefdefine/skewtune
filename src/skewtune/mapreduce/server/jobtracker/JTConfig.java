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
package skewtune.mapreduce.server.jobtracker;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.MRConfig;

/**
 * Place holder for JobTracker server-level configuration.
 * 
 * The keys should have "mapreduce.jobtracker." as the prefix
 */
public interface JTConfig extends MRConfig {
    // JobTracker configuration parameters
    public static final String JT_IPC_ADDRESS = "skewtune.jobtracker.address";

    public static final String JT_HTTP_ADDRESS = "skewtune.jobtracker.http.address";

    public static final String JT_IPC_HANDLER_COUNT = "skewtune.jobtracker.handler.count";

    public static final String JT_RESTART_ENABLED = "skewtune.jobtracker.restart.recover";

    public static final String JT_HEARTBEATS_IN_SECOND = "skewtune.jobtracker.heartbeats.in.second";

    public static final String JT_HEARTBEATS_SCALING_FACTOR = "skewtune.jobtracker.heartbeats.scaling.factor";

    public static final String JT_USER_NAME = org.apache.hadoop.mapreduce.server.jobtracker.JTConfig.JT_USER_NAME;

    public static final String JT_KEYTAB_FILE =
        org.apache.hadoop.mapreduce.server.jobtracker.JTConfig.JT_KEYTAB_FILE;

    public static final String JT_SYSTEM_DIR =
        org.apache.hadoop.mapreduce.server.jobtracker.JTConfig.JT_SYSTEM_DIR;

    public static final String JT_STAGING_AREA_ROOT =
        org.apache.hadoop.mapreduce.server.jobtracker.JTConfig.JT_STAGING_AREA_ROOT;
    
    public static final String JT_MAX_ASYNC_WORKER_FACTOR = "skewtune.jobtracker.asyncworker.maxfactor";
    
    public static final String JT_HEARTBEATS_DUMP = "skewtune.jobtracker.heartbeats.dump";
    
    public static final String JT_SPECULATIVE_SPLIT = "skewtune.jobtracker.speculative.split";
}
