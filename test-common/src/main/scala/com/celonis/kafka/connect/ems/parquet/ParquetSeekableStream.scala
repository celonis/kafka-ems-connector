/*
 * Copyright 2023 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.ems.parquet
import org.apache.parquet.io.DelegatingSeekableInputStream

import java.io.FileInputStream

class ParquetSeekableStream(fileInputStream: FileInputStream) extends DelegatingSeekableInputStream(fileInputStream) {

  var pos: Long = 0

  def getPos: Long = pos

  def seek(newPos: Long): Unit = {
    fileInputStream.getChannel.position(newPos)
    pos = newPos
  }
}
