/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import java.io.File
import java.net.URL

class EmsUploader(url: URL, authorization: String) extends Uploader {
  override def upload(file: File): Unit = {}
}
