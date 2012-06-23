package com.persist

private[persist] class UidGen {
  private var last:Long = 0
  private val delta = 1000 // 1 second
  
  def get:Long = {
    val t = System.currentTimeMillis()
    val result = if (t > last) { t } else { last + 1 }
    last = result 
    result
  }
  
  def set(othert:Long) = {
    val t = System.currentTimeMillis()
    if  (othert > (t + delta))  {
      // TODO report clock skew
    }
    if (othert > t) {
     // set local uid generator forward in time  
      last = othert 
    }
  }
  

}