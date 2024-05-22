/*
 * Copyright 2024 Celonis SE
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

package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DECIMAL_CONVERSION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DECIMAL_CONVERSION_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TRANSFORM_FIELDS_LOWERCASE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TRANSFORM_FIELDS_LOWERCASE_DEFAULT
import com.celonis.kafka.connect.ems.config.PropertiesHelper.getBoolean

final case class PreConversionConfig(convertDecimalsToFloat: Boolean, convertFieldsToLowercase: Boolean)

object PreConversionConfig {
  def extract(props: Map[String, _]): PreConversionConfig =
    PreConversionConfig(
      getBoolean(props, DECIMAL_CONVERSION_KEY).getOrElse(DECIMAL_CONVERSION_DEFAULT),
      getBoolean(props, TRANSFORM_FIELDS_LOWERCASE_KEY).getOrElse(TRANSFORM_FIELDS_LOWERCASE_DEFAULT),
    )
}
