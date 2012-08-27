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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import skewtune.mapreduce.SkewTuneJobConfig;

public class SkewNotifier implements SkewTuneJobConfig {
  private static final Log LOG = LogFactory.getLog(SkewNotifier.class.getName());

  private static Thread thread;
  private static volatile boolean running;
  private static BlockingQueue<SkewStatusInfo> queue = new DelayQueue<SkewStatusInfo>();
  private static String baseUrl;
  
  public synchronized static void startNotifier() {
      if ( running ) {
          LOG.info("skew notifier is already running");
          return; // already started!
      }
      
    running = true;
    thread = new Thread(
                        new Runnable() {
                          public void run() {
                            try {
                              while (running) {
                                sendNotification(queue.take());
                              }
                            }
                            catch (InterruptedException irex) {
                              if (running) {
                                LOG.error("Thread has ended unexpectedly", irex);
                              }
                            }
                          }

                          private void sendNotification(SkewStatusInfo notification) {
                            try {
                              int code = httpNotification(notification.getUri());
                              if (code != 200) {
                                throw new IOException("Invalid response status code: " + code);
                              }
                            }
                            catch (IOException ioex) {
                              LOG.error("Notification failure [" + notification + "]", ioex);
                              if (notification.configureForRetry()) {
                                try {
                                  queue.put(notification);
                                }
                                catch (InterruptedException iex) {
                                  LOG.error("Notification queuing error [" + notification + "]",
                                            iex);
                                }
                              }
                            }
                            catch (Exception ex) {
                              LOG.error("Notification failure [" + notification + "]", ex);
                            }
                          }

                        }

                        );
    
    thread.setDaemon(true);
    thread.start();
    LOG.info("Started skew event notifier");
  }

  public static void stopNotifier() {
    running = false;
    thread.interrupt();
  }
  
  public synchronized static void setBaseUrl(JobConf conf,TaskAttemptID taskid) {
      String uri = conf.get(MAP_INPUT_MONITOR_URL_ATTR);
      if ( uri == null || uri.length() == 0 ) {
          baseUrl = null;
      } else {
          baseUrl = uri + "?i=" + taskid.getTaskID().toString();
      }
  }
  
  public static void registerNotification(float factor,long offset,int len) {
        if ( running && baseUrl != null ) {
            String uri = baseUrl
                    + String.format("&f=%.2f&o=%d&l=%d", factor, offset, len);
            SkewStatusInfo notification = new SkewStatusInfo(uri, 1, 5);
            if (notification != null) {
                try {
                    queue.put(notification);
                } catch (InterruptedException iex) {
                    LOG.error("Notification queuing failure [" + notification
                            + "]", iex);
                }
            }
        }
  }

  private static int httpNotification(String uri) throws IOException {
    URI url = new URI(uri, false);
    HttpClient m_client = new HttpClient();
    HttpMethod method = new GetMethod(url.getEscapedURI());
    method.setRequestHeader("Accept", "*/*");
    return m_client.executeMethod(method);
  }

  private static class SkewStatusInfo implements Delayed {
    private String uri;
    private int retryAttempts;
    private long retryInterval;
    private long delayTime;

    SkewStatusInfo(String uri, int retryAttempts, long retryInterval) {
      this.uri = uri;
      this.retryAttempts = retryAttempts;
      this.retryInterval = retryInterval;
      this.delayTime = System.currentTimeMillis();
    }

    public String getUri() {
      return uri;
    }

    public int getRetryAttempts() {
      return retryAttempts;
    }

    public long getRetryInterval() {
      return retryInterval;
    }

    public long getDelayTime() {
      return delayTime;
    }

    public boolean configureForRetry() {
      boolean retry = false;
      if (getRetryAttempts() > 0) {
        retry = true;
        delayTime = System.currentTimeMillis() + retryInterval;
      }
      retryAttempts--;
      return retry;
    }

    public long getDelay(TimeUnit unit) {
      long n = this.delayTime - System.currentTimeMillis();
      return unit.convert(n, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed d) {
      return (int)(delayTime - ((SkewStatusInfo)d).delayTime);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SkewStatusInfo)) {
        return false;
      }
      if (delayTime == ((SkewStatusInfo)o).delayTime) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return 37 * 17 + (int) (delayTime^(delayTime>>>32));
    }
      
    @Override
    public String toString() {
      return "URL: " + uri + " remaining retries: " + retryAttempts + " interval: " + retryInterval;
    }
  }
}
