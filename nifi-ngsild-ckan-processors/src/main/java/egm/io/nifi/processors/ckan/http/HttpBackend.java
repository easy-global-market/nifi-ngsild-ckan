package egm.io.nifi.processors.ckan.http;

import okhttp3.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
            return createJsonResponse(response.body(), response.headers(), response.code());
        }
    }

    private JsonResponse createJsonResponse(ResponseBody body, Headers headers, int status) throws Exception {
        try {
            logger.info("Http response status: {}", status);

            JSONObject jsonPayload = null;
            if (body != null) {
                String stringBody = body.string();
                logger.info("Http response payload: {}", stringBody);

                if (!stringBody.isEmpty()) {
                    JSONParser jsonParser = new JSONParser();

                    if (stringBody.startsWith("[")) {
                        Object object = jsonParser.parse(stringBody);
                        jsonPayload = new JSONObject();
                        jsonPayload.put("result", object);
                    } else {
                        jsonPayload = (JSONObject) jsonParser.parse(stringBody);
                    }
                }
            }

            String locationHeader = headers.get("Location");

            return new JsonResponse(jsonPayload, status, locationHeader);
        } catch (IllegalStateException e) {
            throw new Exception(e.getMessage());
        }
    }
}
