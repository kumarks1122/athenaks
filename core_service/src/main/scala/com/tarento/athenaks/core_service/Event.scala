package com.tarento.athenaks.core_service

import java.util.Date

class Event(var eid: Int, var eventType: String, var eventTime: Date, var pageId: String) {
  def getID: Int = eid

  def getEventType: String = eventType

  def getEventTime: Date = eventTime

  def getPageId: String = pageId
}