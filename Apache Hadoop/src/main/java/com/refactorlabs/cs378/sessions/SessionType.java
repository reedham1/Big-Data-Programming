package com.refactorlabs.cs378.sessions;

/**
 * Created by davidfranke on 10/25/15.
 */
public enum SessionType {

    SUBMITTER("submitter"),
    CLICKER("clicker"),
    CPO("cpo"),
    SHOWER("shower"),
    VISITOR("visitor"),
    OTHER("other");

    private String text;

    private SessionType(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
