package system.data;

public class BusLine {
    private String lineCode;
    private String lineId;
    private String descriptionGreek;
    private String descriptionEnglish;

    public BusLine(String lineCode, String lineId, String descriptionGreek, String descriptionEnglish) {
        this.lineCode = lineCode;
        this.lineId = lineId;
        this.descriptionGreek = descriptionGreek;
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

    public String getDescriptionGreek() {
        return descriptionGreek;
    }

    public void setDescriptionGreek(String descriptionGreek) {
        this.descriptionGreek = descriptionGreek;
    }

    public String getDescriptionEnglish() {
        return descriptionEnglish;
    }

    public void setDescriptionEnglish(String descriptionEnglish) {
        this.descriptionEnglish = descriptionEnglish;
    }
}
