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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class AccumuloCsvDirectoryInput implements AccumuloCsvInput {
  private static final Logger log = LoggerFactory.getLogger(AccumuloCsvDirectoryInput.class);
  protected final File directory;
  
  public AccumuloCsvDirectoryInput(File dir) {
    Preconditions.checkArgument(dir.exists() && dir.isDirectory());
    
    directory = dir;
  }

  @Override
  public Iterable<File> getInputFiles() {
    Set<File> files = new HashSet<File>();
    getFiles(files, directory);
    return files;
  }
  
  protected void getFiles(Set<File> files, File f) {
    for (String child : f.list()) {
      File childFile = new File(f, child);
      if (files.contains(childFile)) {
        // continue
      } else if (childFile.isDirectory()) {
        getFiles(files, childFile);
      } else if (childFile.isFile()) {
        files.add(childFile);
      } else {
        log.warn("Ignoring {} because it's not a file nor directory.", childFile);
      }
    }
  }
}
