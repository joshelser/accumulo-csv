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
package accumulo.ingest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accumulo.AccumuloCsvOptions;
import accumulo.input.AccumuloCsvInput;
import accumulo.input.AccumuloCsvInputFactory;

/**
 * 
 */
public abstract class AbstractAccumuloCsvIngest implements AccumuloCsvIngest {
  private static final Logger log = LoggerFactory.getLogger(AbstractAccumuloCsvIngest.class);

  protected final AccumuloCsvInput inputs;
  protected final LongLexicoder lex;
  
  public AbstractAccumuloCsvIngest(AccumuloCsvOptions opts) {
    validate(opts);
    
    inputs = AccumuloCsvInputFactory.getInput(opts.getInputFile());
    lex = new LongLexicoder();
  }
  
  protected abstract void validate(AccumuloCsvOptions opts);
  
  protected void setRowId(Text buffer, Text fileName, long recordCount) {
    final byte[] rowSuffix = lex.encode(recordCount);
    buffer.clear();
    buffer.append(fileName.getBytes(), 0, fileName.getLength());
    buffer.append(rowSuffix, 0, rowSuffix.length);
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
