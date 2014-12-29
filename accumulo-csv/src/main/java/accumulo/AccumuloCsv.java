/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package accumulo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accumulo.ingest.AccumuloBulkCsv;
import accumulo.ingest.AccumuloCsvIngest;
import accumulo.ingest.AccumuloLiveCsv;
import accumulo.input.AccumuloCsvOptions;
import accumulo.options.AccumuloBulkCsvOptions;
import accumulo.options.AccumuloLiveCsvOptions;

/**
 *
 */
public class AccumuloCsv {
  private static final Logger log = LoggerFactory.getLogger(AccumuloCsv.class);

  public static void main(String[] args) throws Exception {
    AccumuloCsvOptions opts = new AccumuloCsvOptions();

    // Initial parse to get what type of ingest to run
    opts.parseArgs("Accumulo CSV", args);

    AccumuloCsvIngest task;
    if (opts.isBulkIngest()) {
      AccumuloBulkCsvOptions bulkOpts = new AccumuloBulkCsvOptions();
      bulkOpts.parseArgs("Accumulo Bulk CSV Ingest", args);
      task = new AccumuloBulkCsv(bulkOpts);
    } else if (opts.isLiveIngest()) {
      AccumuloLiveCsvOptions liveOpts = new AccumuloLiveCsvOptions();
      liveOpts.parseArgs("Accumulo Live CSV Ingest", args, new AccumuloLiveCsvOptions());
      task = new AccumuloLiveCsv(liveOpts);
    } else {
      log.warn("Nothing to do!");
      return;
    }

    try {
      task.run();
    } catch (Exception e) {
      log.error("Error running ingest", e);
    } finally {
      task.close();
    }

    return;
  }
}
