package com.example.util

import org.apache.commons.lang3.StringUtils

object S3Util {
  def FormatS3URL(url:String):String={
    if (StringUtils.isEmpty(url) || !url.startsWith("https://")){
      return url
    }
    val substring = url.substring(url.indexOf("//") + 2)
    return  "s3://" + substring.substring(substring.indexOf("/") + 1)
  }
}
