package egm.io.nifi.processors.ckan.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBackend {
    private static final Logger logger = LoggerFactory.getLogger(HttpBackend.class);
    private final String url;
    private final OkHttpClient httpClient;

    public HttpBackend(String url) {
        this.url = url;

        httpClient = new OkHttpClient();
    }

    protected JsonResponse doRequest(String method, String path, Headers headers, String body)
            throws Exception {

        Request.Builder requestBuilder = new Request.Builder().url(url + path);
        requestBuilder = switch (method) {
            case "PUT" -> requestBuilder.put(RequestBody.create(body, MediaType.get("application/json")));
            case "POST" -> requestBuilder.post(RequestBody.create(body, MediaType.get("application/json")));
            case "GET" -> requestBuilder;
            case "DELETE" -> requestBuilder.delete();
            case null, default -> throw new Exception("HTTP method not supported: " + method);
        };

        if (headers != null) {
            requestBuilder.headers(headers);
        }

        Request request = requestBuilder.build();
        logger.info("Http request: {}", request);

        try (Response response = httpClient.newCall(request).execute()) {
            return createJsonResponse(response.body(), response.code());
        }
    }

    private JsonResponse createJsonResponse(ResponseBody body, int status) throws Exception {
        try {
            logger.info("Http response status: {}", status);

            JsonObject jsonPayload = null;
            if (body != null) {
                String stringBody = body.string();
                logger.info("Http response payload: {}", stringBody);

                if (!stringBody.isEmpty()) {
                    if (stringBody.startsWith("[")) {
                        JsonArray array = JsonParser.parseString(stringBody).getAsJsonArray();
                        jsonPayload = new JsonObject();
                        jsonPayload.add("result", array);
                    } else {
                        jsonPayload = JsonParser.parseString(stringBody).getAsJsonObject();
                    }
                }
            }

            return new JsonResponse(jsonPayload, status);
        } catch (IllegalStateException e) {
            throw new Exception(e.getMessage());
        }
    }
}
