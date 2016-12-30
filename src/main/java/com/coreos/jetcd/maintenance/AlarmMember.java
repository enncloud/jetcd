package com.coreos.jetcd.maintenance;

/**
 * Alarm member.
 */
public class AlarmMember {
    // memberID is the ID of the member associated with the raised alarm.
    public final long memberID;
    // alarm is the type of alarm which has been raised.
    public final AlarmType type;

    public AlarmMember(long memberID, AlarmType type) {
        this.memberID = memberID;
        this.type = type;
    }
}
