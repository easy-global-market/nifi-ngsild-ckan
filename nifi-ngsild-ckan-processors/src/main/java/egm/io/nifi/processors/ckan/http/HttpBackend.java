package egm.io.nifi.processors.ckan.http;

import okhttp3.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HttpBackend {
    private final String url;
    private final OkHttpClient httpClient;
    private static final Logger logger = LoggerFactory.getLogger(HttpBackend.class);

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
        logger.debug("Http request: {}", request);

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful())
                throw new IOException("Unexpected response status: " + response);
            assert response.body() != null;
            return createJsonResponse(response.body().string(), response.headers(), response.code());
        } catch (IOException e) {
            throw new Exception(e.getMessage());
        }
    }

    private JsonResponse createJsonResponse(String body, Headers headers, int status) throws Exception {
        try {
            logger.info("Http response status: {}", status);

            JSONObject jsonPayload = null;
            if (body != null) {
                logger.debug("Http response payload: {}", body);

                if (!body.isEmpty()) {
                    JSONParser jsonParser = new JSONParser();

                    if (body.startsWith("[")) {
                        Object object = jsonParser.parse(body);
                        jsonPayload = new JSONObject();
                        jsonPayload.put("result", object);
                    } else {
                        jsonPayload = (JSONObject) jsonParser.parse(body);
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
