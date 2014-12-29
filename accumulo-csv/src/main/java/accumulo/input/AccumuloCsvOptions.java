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
package accumulo.input;

import java.io.File;

import org.apache.accumulo.core.cli.ClientOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 *
 */
public class AccumuloCsvOptions extends ClientOpts {
  private static final Logger log = LoggerFactory.getLogger(AccumuloCsvOptions.class);

  private static final String LIVE = "--live", BULK = "--bulk";

  @Parameter(names = LIVE, required = false, description = "Write mutations to Accumulo")
  private boolean liveIngest;

  @Parameter(names = BULK, required = false, description = "Create offline Accumulo files for import")
  private boolean bulkIngest;

  @Parameter(names = {"-f", "--file"}, required = true, description = "Input CSV file(s)")
  private File inputFile;

  public boolean isLiveIngest() {
    return liveIngest;
  }

  public boolean isBulkIngest() {
    return bulkIngest;
  }

  public File getInputFile() {
    return inputFile;
  }

  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
    checkExclusiveIngestTypes();
    if (isLiveIngest()) {
      validateLiveIngestOptions();
    }
  }

  /**
   * One of Live or Bulk ingest are required and both are exclusive
   */
  protected void checkExclusiveIngestTypes() {
    if ((isBulkIngest() && isLiveIngest()) || (!isBulkIngest() && !isLiveIngest())) {
      log.error("Must chose one of {} or {}", LIVE, BULK);
      System.exit(1);
      return;
    }
  }

  protected void validateLiveIngestOptions() {

  }
}
