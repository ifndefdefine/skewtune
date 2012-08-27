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

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SpeculationEventNotifier {
  private static final Log LOG =
    LogFactory.getLog(SpeculationEventNotifier.class.getName());

  private static Thread thread;
  private static volatile boolean running;
  private static BlockingQueue<SpeculationStatusInfo> queue = new DelayQueue<SpeculationStatusInfo>();
  
  public static final String SPECULATION_EVENT_URI = "skewreduce.speculative.notify.uri";

  public static void startNotifier() {
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

                          private void sendNotification(SpeculationStatusInfo notification) {
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
    LOG.info("Started speculation event notifier");
  }

  public static void stopNotifier() {
    running = false;
    thread.interrupt();
  }

  private static SpeculationStatusInfo createNotification(JobConf conf,TaskInProgress tip) {
    SpeculationStatusInfo notification = null;
    String uri = conf.get(SPECULATION_EVENT_URI);
    
    if (uri != null) {
      // +1 to make logic for first notification identical to a retry
      int retryAttempts = conf.getInt(JobContext.END_NOTIFICATION_RETRIES, 0) + 1;
      long retryInterval = conf.getInt(JobContext.END_NOTIFICATION_RETRIE_INTERVAL, 30000);
      if (uri.contains("$jobId")) {
        uri = uri.replace("$jobId", tip.getTIPId().getJobID().toString());
      }
//      tip.getCurrentProgressRate(now);
//      double t1 = tip.getCurrentProgressRate(now) / Math.max(0.0001,1.0 - tip.getProgress());
//      if (uri.contains("$taskProgress")) {
//        String statusStr = String.format("%.6g", tip.getProgress());
//        uri = uri.replace("$taskProgress", statusStr);
//      }
//      
//      if ( uri.contains("$taskRemainTime")) {
//          double t = tip.getCurrentProgressRate(JobTracker.getClock().getTime()) / Math.max(0.0001,1.0 - tip.getProgress());
//          String statusStr = String.format("%.6g", t);
//          uri = uri.replace("$taskRemainTime", statusStr);
//      }

      notification = new SpeculationStatusInfo(uri, retryAttempts, retryInterval);
    }
    return notification;
  }

  public static void registerNotification(JobConf jobConf, TaskInProgress tip) {
    SpeculationStatusInfo notification = createNotification(jobConf, tip);
    if (notification != null) {
      try {
        queue.put(notification);
      }
      catch (InterruptedException iex) {
        LOG.error("Notification queuing failure [" + notification + "]", iex);
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

  // for use by the LocalJobRunner, without using a thread&queue,
  // simple synchronous way
  public static void localRunnerNotification(JobConf conf, TaskInProgress tip) {
    SpeculationStatusInfo notification = createNotification(conf, tip);
    if (notification != null) {
      while (notification.configureForRetry()) {
        try {
          int code = httpNotification(notification.getUri());
          if (code != 200) {
            throw new IOException("Invalid response status code: " + code);
          }
          else {
            break;
          }
        }
        catch (IOException ioex) {
          LOG.error("Notification error [" + notification.getUri() + "]", ioex);
        }
        catch (Exception ex) {
          LOG.error("Notification error [" + notification.getUri() + "]", ex);
        }
        try {
          Thread.sleep(notification.getRetryInterval());
        }
        catch (InterruptedException iex) {
          LOG.error("Notification retry error [" + notification + "]", iex);
        }
      }
    }
  }

  private static class SpeculationStatusInfo implements Delayed {
    private String uri;
    private int retryAttempts;
    private long retryInterval;
    private long delayTime;

    SpeculationStatusInfo(String uri, int retryAttempts, long retryInterval) {
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
      return (int)(delayTime - ((SpeculationStatusInfo)d).delayTime);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SpeculationStatusInfo)) {
        return false;
      }
      if (delayTime == ((SpeculationStatusInfo)o).delayTime) {
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
      return "URL: " + uri + " remaining retries: " + retryAttempts +
        " interval: " + retryInterval;
    }
  }
}
