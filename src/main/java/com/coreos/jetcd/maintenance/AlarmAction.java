package com.coreos.jetcd.maintenance;

/**
 * action is the kind of alarm request to issue. The action
 * may GET alarm statuses, ACTIVATE an alarm, or DEACTIVATE a
 * raised alarm.
 */
public enum AlarmAction {
    GET, ACTIVATE, DEACTIVATE
}
