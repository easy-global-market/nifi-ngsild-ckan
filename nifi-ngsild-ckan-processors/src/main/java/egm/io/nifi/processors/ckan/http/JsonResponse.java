package egm.io.nifi.processors.ckan.http;

import org.json.simple.JSONObject;

public record JsonResponse(
        JSONObject jsonObject,
        int statusCode,
        String locationHeader
) {
    @Override
    public JSONObject jsonObject() {
        return jsonObject;
    } // getJsonObject

    @Override
    public int statusCode() {
        return statusCode;
    } // getStatusCode

    @Override
    public String locationHeader() {
        return locationHeader;
    }
}
