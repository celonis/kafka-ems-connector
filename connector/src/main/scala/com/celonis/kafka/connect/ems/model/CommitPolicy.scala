/*
 * Copyright 2022 Celonis SE
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

package com.celonis.kafka.connect.ems.model

import org.slf4j.Logger

/** The [[CommitPolicy]] is responsible for determining when a file should be flushed (closed on disk, and moved to be
  * visible).
  *
  * Typical implementations will flush based on number of records, file size, or time since the file was opened.
  */
trait CommitPolicy {

  /** This method is invoked after a file has been written.
    *
    * If the output file should be committed at this time, then this method should return true, otherwise false.
    *
    * Once a commit has taken place, a new file will be opened for the next record.
    */
  def shouldFlush(context: CommitContext): Boolean
}

/** @param count
  *   \- The number of records written thus far to the file
  * @param lastWrite
  *   \- The time in milliseconds when the the file was created/written-to
  */
case class CommitContext(
  count:     Long,
  fileSize:  Long,
  lastWrite: Long,
)

/** Default implementation of [[CommitPolicy]] that will flush the output file under the following circumstances:
  *   - file size reaches limit
  *   - time since file was created
  *   - number of files is reached
  *
  * @param interval
  *   in millis
  */
case class DefaultCommitPolicy(fileSize: Long, interval: Long, records: Long) extends CommitPolicy {
  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def shouldFlush(context: CommitContext): Boolean = {
    val timeSinceLastWrite = System.currentTimeMillis() - context.lastWrite
    val flushDueToFileSize = fileSize <= context.fileSize
    val flushDueToInterval = interval <= timeSinceLastWrite
    val flushDueToCount    = records <= context.count

    val flush = flushDueToFileSize || flushDueToInterval || flushDueToCount

    if (flush)
      logger.debug(
        s"Flushing: size: $flushDueToFileSize, interval: $flushDueToInterval, count: $flushDueToCount, CommitContext: $context",
      )

    flush
  }
}
