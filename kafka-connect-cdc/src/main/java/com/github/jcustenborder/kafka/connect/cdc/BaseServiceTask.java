/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class BaseServiceTask<CONF extends CDCSourceConnectorConfig> extends CDCSourceTask<CONF> {
  private static final Logger log = LoggerFactory.getLogger(BaseServiceTask.class);
  ServiceManager serviceManager;

  protected abstract Service service(ChangeWriter changeWriter, OffsetStorageReader offsetStorageReader);

  @Override
  public void start(Map<String, String> map) {
    log.info("Starting BaseServiceTask...");

    super.start(map);
    Service service = service(this, this.context.offsetStorageReader());
    List<Service> services = Arrays.asList(service);
    this.serviceManager = new ServiceManager(services);
    this.serviceManager.startAsync();

    try {
      this.serviceManager.awaitHealthy(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException("Timeout while starting service.", e);
    }
  }

  @Override
  public void stop() {
    log.info("Stopping BaseServiceTask...");
    this.serviceManager.stopAsync();

    try {
      this.serviceManager.awaitStopped(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException("Timeout while stopping service.", e);
    }

  }
}
