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

import com.beust.jcommander.JCommander;

/**
 * 
 */
public class AccumuloCsv {
  private static final Logger log = LoggerFactory.getLogger(AccumuloCsv.class);
  
  public static void main(String[] args) throws Exception {
    AccumuloCsvOptions opts = new AccumuloCsvOptions();
    JCommander jc = new JCommander(opts);
    
    jc.setProgramName("Accumulo CSV");
    
    jc.parse(args);
    
    if ((opts.isBulkIngest() && opts.isLiveIngest()) || (!opts.isBulkIngest() && !opts.isLiveIngest())) {
      log.error("Must chose one of --live or --bulk");
      System.exit(1);
      return;
    }
    
    AccumuloCsvIngest task;
    if (opts.isBulkIngest()) {
      task = new AccumuloBulkCsv(opts);
    } else if (opts.isLiveIngest()) {
      task = new AccumuloLiveCsv(opts);
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
