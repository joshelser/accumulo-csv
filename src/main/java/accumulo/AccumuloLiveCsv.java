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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 
 */
public class AccumuloLiveCsv implements Runnable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(AccumuloLiveCsv.class);
  private static final String ROW_SEPARATOR = ":";

  protected final ZooKeeperInstance instance;
  protected final Connector connector;
  protected final AccumuloCsvInput inputs;
  protected final String recordTableName, schemaTableName;
  protected final MultiTableBatchWriter mtbw;

  public AccumuloLiveCsv(AccumuloCsvOptions opts) throws AccumuloException, AccumuloSecurityException {
    validate(opts);

    instance = new ZooKeeperInstance(opts.getInstanceName(), opts.getZookeepers());
    connector = instance.getConnector(opts.getUsername(), new PasswordToken(opts.getPassword()));

    inputs = AccumuloCsvInputFactory.getInput(opts.getInputFile());

    recordTableName = opts.getTablePrefix() + "_records";
    schemaTableName = opts.getTablePrefix() + "_schema";

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(recordTableName)) {
      try {
        tops.create(recordTableName);
      } catch (TableExistsException e) {
        // yay, race conditions
      }
    }

    if (!tops.exists(schemaTableName)) {
      try {
        tops.create(schemaTableName);
      } catch (TableExistsException e) {
        // yay, race conditions
      }
    }

    mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig());
  }

  @Override
  public void close() throws IOException {
    try {
      mtbw.close();
    } catch (MutationsRejectedException e) {
      log.error("Error committing mutations on close", e);
      throw new IOException(e);
    }
  }

  protected void validate(AccumuloCsvOptions opts) {
    if (null == opts.getInstanceName() || null == opts.getZookeepers() || null == opts.getUsername() || null == opts.getPassword()) {
      log.error("Must provide instance name, zookeepers, username and password");
      System.exit(1);
      return;
    }

    File inputFile = opts.getInputFile();
    if (null == inputFile || !inputFile.exists()) {
      log.error("Must provide input file or directory");
      System.exit(2);
      return;
    }

    if (StringUtils.isBlank(opts.getTablePrefix())) {
      log.error("Must provide non-empty table prefix");
      System.exit(3);
      return;
    }
  }

  @Override
  public void run() {
    FileReader fileReader = null;
    CSVReader reader = null;
    final Text rowId = new Text();
    
    for (File f : inputs.getInputFiles()) {
      rowId.clear();
      
      String fileName;
      try {
        fileName = f.getCanonicalPath() + ROW_SEPARATOR;
      } catch (IOException e) {
        log.error("Could not determine path for file: {}", f, e);
        continue;
      }
      
      try {
        try {
          fileReader = new FileReader(f);
        } catch (FileNotFoundException e) {
          log.error("Could not read file {}", f.toString());
          continue;
        }

        reader = new CSVReader(fileReader);

        String[] header;
        try {
          header = reader.readNext();
        } catch (IOException e) {
          log.error("Error reading header", e);
          continue;
        }

        try {
          writeSchema(header);
        } catch (AccumuloException e) {
          log.error("Could not write header to schema table", e);
          continue;
        } catch (AccumuloSecurityException e) {
          log.error("Could not write header to schema table", e);
          continue;
        }
        
        String[] record;
        long recordCount = 0l;
        
        try {
          while (null != (record = reader.readNext())) {
            // Make a rowId of "/path/to/filename:N" 
            final String rowSuffix = Long.toString(recordCount);
            rowId.append(fileName.getBytes(), 0, fileName.length());
            rowId.append(rowSuffix.getBytes(), 0, rowSuffix.length());
            
            try {
              writeRecord(header, record, rowId);
            } catch (AccumuloException e) {
              
            } catch (AccumuloSecurityException e) {
              
            }
            
            recordCount++;
          }
        } catch (IOException e) {
          log.error("Error reading records from CSV file", e);
          continue;
        }
        
      } finally {
        if (null != reader) {
          try {
            reader.close();
          } catch (IOException e) {
            log.error("Error closing CSV reader", e);
          }
        }

        if (null != fileReader) {
          try {
            fileReader.close();
          } catch (IOException e) {
            log.error("Error closing file reader", e);
          }
        }
      }
    }
  }

  protected void writeSchema(String[] header) throws AccumuloException, AccumuloSecurityException {
    final BatchWriter bw;
    try {
      bw = mtbw.getBatchWriter(schemaTableName);
    } catch (TableNotFoundException e) {
      log.error("Schema table ({}) was deleted", schemaTableName, e);
      throw new RuntimeException(e);
    }

    // do stuff
  }

  protected void writeRecord(String[] header, String[] record, Text rowId) throws AccumuloException, AccumuloSecurityException {
    final BatchWriter recordBw, schemaBw;
    try {
      recordBw = mtbw.getBatchWriter(recordTableName);
      schemaBw = mtbw.getBatchWriter(schemaTableName);
    } catch (TableNotFoundException e) {
      log.error("Table(s) ({}, {}) were deleted", recordTableName, schemaTableName, e);
      throw new RuntimeException(e);
    }

  }

}
