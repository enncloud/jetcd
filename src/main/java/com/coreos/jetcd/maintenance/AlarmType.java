package com.coreos.jetcd.maintenance;

/**
 * Alarm type.
 */
public enum AlarmType {
    // default, used to query if any alarm is active
    NONE,
    // space quota is exhausted
    NOSPACE
}
