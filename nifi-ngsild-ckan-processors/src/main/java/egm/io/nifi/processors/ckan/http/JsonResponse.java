package egm.io.nifi.processors.ckan.http;

import com.google.gson.JsonObject;

public record JsonResponse(
    JsonObject jsonObject,
    int statusCode
) {
    @Override
    public JsonObject jsonObject() {
        return jsonObject;
    }

    @Override
    public int statusCode() {
        return statusCode;
    }
}
