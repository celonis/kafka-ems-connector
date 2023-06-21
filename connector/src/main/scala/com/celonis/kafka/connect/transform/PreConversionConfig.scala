package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DECIMAL_CONVERSION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DECIMAL_CONVERSION_KEY_DEFAULT
import com.celonis.kafka.connect.ems.config.PropertiesHelper.getBoolean

final case class PreConversionConfig(convertDecimalsToFloat: Boolean)

object PreConversionConfig {
  def extract(props: Map[String, _]): PreConversionConfig =
    PreConversionConfig(getBoolean(props, DECIMAL_CONVERSION_KEY).getOrElse(DECIMAL_CONVERSION_KEY_DEFAULT))
}
