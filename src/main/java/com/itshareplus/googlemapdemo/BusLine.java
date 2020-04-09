package com.itshareplus.googlemapdemo;

import java.io.Serializable;

public class BusLine implements Serializable {
    private String lineCode;
    private String lineId;
    private String descriptionEnglish;

    public BusLine(String lineCode, String lineId, String descriptionEnglish) {
        this.lineCode = lineCode;
        this.lineId = lineId;
        this.descriptionEnglish = descriptionEnglish;
    }

    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
    }

    public String getLineId() {
        return lineId;
    }

    public void setLineId(String lineId) {
        this.lineId = lineId;
    }

    public String getDescriptionEnglish() {
        return descriptionEnglish;
    }

    public void setDescriptionEnglish(String descriptionEnglish) {
        this.descriptionEnglish = descriptionEnglish;
    }
}
