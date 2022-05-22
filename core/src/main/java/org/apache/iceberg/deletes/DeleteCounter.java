/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.deletes;

/**
 * A counter to be used to count deletes as they are applied.
 */
public interface DeleteCounter {

  /**
   * Return an instance of DeleteCounter.
   */
  static DeleteCounter instance() {
    return new StandardCounter();
  }

  /**
   * Return an instance of DeleteCounter that is not to be used.
   */
  static DeleteCounter none() {
    return new NonCounter();
  }

  /**
   * Increment the counter by one.
   */
  void increment();

  /**
   * Increment the counter by delta.
   */
  void increment(long delta);

  /**
   * Return the current value of the counter.
   */
  long get();
}
