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

import java.io.File;

import com.beust.jcommander.Parameter;

/**
 * 
 */
public class AccumuloCsvOptions {

  @Parameter(names = "--live", required = false, description = "Write mutations to Accumulo")
  private boolean liveIngest;

  @Parameter(names = "--bulk", required = false, description = "Create offline Accumulo files for import")
  private boolean bulkIngest;
  
  @Parameter(names = {"-f", "--file"}, required = true, description = "Input CSV file(s)")
  private File inputFile;
  
  @Parameter(names = {"-zk", "--zookeepers"}, required = false, description = "CSV of ZooKeepers")
  private String zookeepers;
  
  @Parameter(names = {"-i", "--instance"}, required = false, description = "Accumulo instance name")
  private String instanceName;
  
  @Parameter(names = {"-u", "--user"}, required = false, description = "Accumulo user name")
  private String username;
  
  @Parameter(names = {"-p", "--password"}, required = false, description = "Accumulo password")
  private String password;
  
  @Parameter(names = {"-t", "--tablePrefix"}, required = false, description = "Prefix to use for Accumulo table names")
  private String tablePrefix;
  
  public boolean isLiveIngest() {
    return liveIngest;
  }

  public boolean isBulkIngest() {
    return bulkIngest;
  }

  public File getInputFile() {
    return inputFile;
  }
  
  public String getZookeepers() {
    return zookeepers;
  }
  
  public String getInstanceName() {
    return instanceName;
  }
  
  public String getUsername() {
    return username;
  }
  
  public String getPassword() {
    return password;
  }
  
  public String getTablePrefix() {
    return tablePrefix;
  }
}
