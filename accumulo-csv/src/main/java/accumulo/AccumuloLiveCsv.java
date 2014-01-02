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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class AccumuloLiveCsv implements AccumuloCsvIngest {
  private static final Logger log = LoggerFactory.getLogger(AccumuloLiveCsv.class);
  private static final String ROW_SEPARATOR = ":";

  public static final Text SCHEMA_COLUMN = new Text("col");
  public static final Text SCHEMA_COLUMN_FREQ = new Text("freq");
  public static final Text EMPTY_TEXT = new Text(new byte[0]);
  public static final Value VALUE_ONE = new Value("1".getBytes());

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
        throw new RuntimeException(e);
      }
    }

    if (!tops.exists(schemaTableName)) {
      try {
        tops.create(schemaTableName);
      } catch (TableExistsException e) {
        // yay, race conditions
        throw new RuntimeException(e);
      }
    }
    
    // Sum all values in the 'freq' column, before the VersioningIterator
    IteratorSetting aggregation = new IteratorSetting(19, SummingCombiner.class);
    SummingCombiner.setColumns(aggregation, Collections.<Column> singletonList(new Column(SCHEMA_COLUMN_FREQ)));
    SummingCombiner.setEncodingType(aggregation, Type.VARLEN);
    
    try {
      tops.attachIterator(schemaTableName, aggregation);
    } catch (TableNotFoundException e) {
      // yay, race conditions
      throw new RuntimeException(e);
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
    long totalRecordsInserted = 0;

    for (File f : inputs.getInputFiles()) {
      String absoluteFileName;
      try {
        absoluteFileName = f.getCanonicalPath();
      } catch (IOException e) {
        log.error("Could not determine path for file: {}", f, e);
        continue;
      }
      
      log.info("Starting to process {}", absoluteFileName);
      
      absoluteFileName += ROW_SEPARATOR;
      Text fileName = new Text(absoluteFileName);

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
          writeSchema(fileName, header);
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
            rowId.clear();
            rowId.append(fileName.getBytes(), 0, fileName.getLength());
            rowId.append(rowSuffix.getBytes(), 0, rowSuffix.length());

            try {
              writeRecord(header, record, rowId, fileName);
            } catch (AccumuloException e) {
              log.error("Could not write record to record table", e);
            } catch (AccumuloSecurityException e) {
              log.error("Could not write record to record table", e);
            }

            recordCount++;
            totalRecordsInserted++;
            
            if (0 == totalRecordsInserted % 1000) {
              mtbw.flush();
            }
          }
        } catch (IOException e) {
          log.error("Error reading records from CSV file", e);
          continue;
        } catch (MutationsRejectedException e) {
          log.error("Error flushing mutations to server", e);
          throw new RuntimeException(e);
        } finally {
          log.info("Processed {} records from {}", recordCount, absoluteFileName);
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
    
    log.info("Processed {} records in total", totalRecordsInserted);
  }

  protected void writeSchema(Text fileName, String[] header) throws AccumuloException, AccumuloSecurityException {
    final BatchWriter bw;
    try {
      bw = mtbw.getBatchWriter(schemaTableName);
    } catch (TableNotFoundException e) {
      log.error("Schema table ({}) was deleted", schemaTableName, e);
      throw new RuntimeException(e);
    }

    final Value emptyValue = new Value(new byte[0]);
    // track existence of column in schema
    for (String columnName : header) {
      Mutation m = new Mutation(columnName);
      m.put(SCHEMA_COLUMN, fileName, emptyValue);

      bw.addMutation(m);
    }
  }

  protected void writeRecord(String[] header, String[] record, Text rowId, Text fileName) throws AccumuloException, AccumuloSecurityException {
    Preconditions.checkArgument(header.length >= record.length, "Cannot have more columns in record (%s) than defined in header (%s)", 
        new Object[] { header.length, record.length});
    
    final BatchWriter recordBw, schemaBw;
    try {
      recordBw = mtbw.getBatchWriter(recordTableName);
      schemaBw = mtbw.getBatchWriter(schemaTableName);
    } catch (TableNotFoundException e) {
      log.error("Table(s) ({}, {}) were deleted", recordTableName, schemaTableName, e);
      throw new RuntimeException(e);
    }

    // Some temp Texts to avoid lots of object allocations
    final Text cfHolder = new Text();
    final HashMap<String,Long> counts = new HashMap<String,Long>();
    
    // write records
    Mutation recordMutation = new Mutation(rowId);
    for (int i = 0; i < record.length; i++) {
      final String columnName = header[i];
      final String columnValue = record[i];
      
      if (counts.containsKey(columnName)) {
        counts.put(columnName, counts.get(columnName) + 1);
      } else {
        counts.put(columnName, 1l);
      }
      
      cfHolder.set(columnName);
      
      recordMutation.put(cfHolder, EMPTY_TEXT, new Value(columnValue.getBytes()));
    }
    
    recordBw.addMutation(recordMutation);
    
    // update counts in schema
    for (Entry<String,Long> schemaUpdate : counts.entrySet()) {
      Mutation schemaMutation = new Mutation(schemaUpdate.getKey());
      
      schemaMutation.put(SCHEMA_COLUMN_FREQ, fileName, longToValue(schemaUpdate.getValue()));
      schemaBw.addMutation(schemaMutation);
    }
  }
  
  protected Value longToValue(long l) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    try {
      WritableUtils.writeVLong(dos, l);
    } catch (IOException e) {
      log.error("IOException writing to a byte array...", e);
      throw new RuntimeException(e);
    }
    
    return new Value(baos.toByteArray());
  }
}
