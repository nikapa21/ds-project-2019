package system.data;

public class RouteCode {
    private String lineCode;
    private String routeCode;
    private String descriptionGreek;
    private String descriptionEnglish;

    public RouteCode(String lineCode, String routeCode, String descriptionGreek, String descriptionEnglish) {
        this.lineCode = lineCode;
        this.routeCode = routeCode;
        this.descriptionGreek = descriptionGreek;
        this.descriptionEnglish = descriptionEnglish;
    }

    public String getLineCode() {
        return lineCode;
    }

    public void setLineCode(String lineCode) {
        this.lineCode = lineCode;
    }

    public String getRouteCode() {
        return routeCode;
    }

    public void setRouteCode(String routeCode) {
        this.routeCode = routeCode;
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


