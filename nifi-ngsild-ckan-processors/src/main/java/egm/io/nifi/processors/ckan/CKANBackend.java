package egm.io.nifi.processors.ckan;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import egm.io.nifi.processors.ckan.http.HttpBackend;
import egm.io.nifi.processors.ckan.http.JsonResponse;
import egm.io.nifi.processors.ckan.model.DataStore;
import egm.io.nifi.processors.ckan.model.DCATMetadata;
import egm.io.nifi.processors.ckan.ngsild.*;
import egm.io.nifi.processors.ckan.utils.CKANCache;
import org.apache.http.Header;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CKANBackend extends HttpBackend {

    private final String apiKey;
    private final String viewer;
    private final CKANCache cache;
    private static final Logger logger = LoggerFactory.getLogger(CKANBackend.class);
    final NGSIUtils ngsiUtils = new NGSIUtils();

    public CKANBackend(String apiKey, String[] ckanHost, String ckanPort,
            boolean ssl, String ckanViewer) {
        super(ckanHost, ckanPort, ssl);

        // this class attributes
        this.apiKey = apiKey;
        this.viewer = ckanViewer;

        // create the cache
        cache = new CKANCache(ckanHost, ckanPort, ssl, apiKey);
    } // CKANBackendImpl

    public void persist(String orgName, String pkgName, String pkgTitle, String resName, String records, DCATMetadata dcatMetadata, boolean createDataStore)
            throws Exception {

        logger.info("Going to lookup for the resource id, the cache may be updated during the process (orgName=\"{}\", " +
                "pkgName=\"{}\", resName=\"{}\"" , orgName, pkgName, resName);

        String resId = resourceLookupOrCreateDynamicFields(orgName, pkgName, pkgTitle, resName,records, dcatMetadata,createDataStore);
        if (resId == null) {
            throw new Exception("Cannot persist the data (orgName=" + orgName + ", pkgName=" + pkgName
                    + ", resName=" + resName + ")");
        } else {
            if (createDataStore){

                logger.info("Going to persist the data (orgName=\"{}\", pkgName=\"{}\", resName/resId=\"{}/{}\")", orgName, pkgName, resName, resId);

                insert(resId, records);
            }
            else {

                logger.info("DataStore was not created in the resource (orgName=\"{}\", pkgName=\"{}\", resName/resId=\"{}/{}\")", orgName, pkgName, resName, resId);

            }
        } // if else
    } // persist

    /**
     * Lookup or create resources used by cygnus-ngsi-ld and create the datastore with the fields available in the records parameter.
     * @param orgName The organization in which datastore the record is going to be inserted
     * @param pkgName The package name to be created or lookup to
     * @param resName The resource name to be created or lookup to
     * @param records Te records to be inserted and used to create the datastore fields
     */
    private String resourceLookupOrCreateDynamicFields(String orgName, String pkgName, String pkgTitle, String resName, String records,DCATMetadata dcatMetadata, boolean createDataStore)
            throws Exception {
        if (!cache.isCachedOrg(orgName)) {
            logger.info("The organization was not cached nor existed in CKAN (orgName=\"{}\")", orgName);
            String orgId = createOrganization(orgName,dcatMetadata);
            cache.addOrg(orgName);
            cache.setOrgId(orgName, orgId);
            logger.info("Created new organization in CKAN (orgName=\"{}\", orgId=\"{}\")", orgName, orgId);

            String pkgId = createPackage(pkgName, pkgTitle, orgId, dcatMetadata);
            cache.addPkg(orgName, pkgName);
            cache.setPkgId(orgName, pkgName, pkgId);
            String resId = createResource(resName, pkgId, dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId,resName,records);
                createView(resId);
            }
            return resId;
            // if else
        } // if

        logger.info("The organization was cached (orgName=\"{}\")", orgName);

        if (!cache.isCachedPkg(orgName, pkgName)) {
            logger.info("The package was not cached nor existed in CKAN (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);

            String pkgId = createPackage(pkgName, pkgTitle, cache.getOrgId(orgName), dcatMetadata);
            cache.addPkg(orgName, pkgName);
            cache.setPkgId(orgName, pkgName, pkgId);
            String resId = createResource(resName, pkgId, dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId,resName,records);
                createView(resId);
            }
            return resId;

        } // if

        logger.info("The package was cached (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);

        if (!cache.isCachedRes(orgName, pkgName, resName)) {
            logger.info("The resource was not cached nor existed in CKAN (orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")" ,orgName, pkgName, resName);

            String resId = this.createResource(resName, cache.getPkgId(orgName, pkgName), dcatMetadata);
            cache.addRes(orgName, pkgName, resName);
            cache.setResId(orgName, pkgName, resName, resId);
            if(createDataStore){
                createDataStoreWithFields(resId, resName, records);
                createView(resId);
            }
            return resId;

        } // if

        logger.info("The resource was cached (orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);

        return cache.getResId(orgName, pkgName, resName);
    } // resourceLookupOrCreateDynamicFields


    /**
     * Insert records in the datastore.
     * @param resId The resource in which datastore the record is going to be inserted
     * @param records Records to be inserted in Json format
     */
    private void insert(String resId, String records) throws Exception {
        String jsonString = "{ \"resource_id\": \"" + resId
                + "\", \"records\": [ " + records + " ], "
                + "\"method\": \"insert\", "
                + "\"force\": \"true\" }";

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_upsert";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            logger.info("Successful insert (resource/datastore id=\"{}\")", resId);
        } else {
            throw new Exception("Could not insert (resId=" + resId + ", statusCode="
                    + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // insert

    /**
     * Creates an organization in CKAN.
     * @param orgName Organization to be created
     * @return The organization id
     */
    private String createOrganization(String orgName, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON
        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("name",orgName);

        if (dcatMetadata!=null){
            dataJson.addProperty("title",orgName);
            dataJson.addProperty("name",orgName);

            //dataJson.add("extras",extrasJsonArray);
        }
        logger.debug("dataJson: {}",dataJson);

        // create the CKAN request URL
        String urlPath = "/api/3/action/organization_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String orgId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            logger.info("Successful organization creation (orgName/OrgId=\"{}/{}\")", orgName, orgId);
            return orgId;
        } else {
            throw new Exception("Could not create the orgnaization (orgName=" + orgName
                    + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // createOrganization

    /**
     * Creates a dataset/package within a given organization in CKAN.
     * @param pkgName Package to be created
     * @param pkgTitle Package title
     * @param orgId Organization the package belongs to
     * @return A package identifier if the package was created or an exception if something went wrong
     */
    private String createPackage(String pkgName, String pkgTitle, String orgId, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON
        JsonArray extrasJsonArray = new JsonArray();
        JsonArray tagsJsonArray = new JsonArray();
        String[] keywords;
        JsonObject extrasJson = new JsonObject();
        JsonObject tags;

        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("name",pkgName);
        dataJson.addProperty("owner_org",orgId);
        dataJson.addProperty("title", pkgTitle);
        if (dcatMetadata!=null){
            dataJson.addProperty("notes",dcatMetadata.getPackageDescription());
            dataJson.addProperty("version",dcatMetadata.getVersion());
            dataJson.addProperty("url",dcatMetadata.getLandingPage());
            dataJson.addProperty("visibility",dcatMetadata.getVisibility());
            dataJson.addProperty("url",dcatMetadata.getLandingPage());


            extrasJson.addProperty("key","publisher_type");
            extrasJson.addProperty("value",dcatMetadata.getOrganizationType());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_uri");
            extrasJson.addProperty("value",dcatMetadata.getContactPoint());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_name");
            extrasJson.addProperty("value",dcatMetadata.getContactName());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","contact_email");
            extrasJson.addProperty("value",dcatMetadata.getContactEmail());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","spatial_uri");
            extrasJson.addProperty("value",dcatMetadata.getSpatialUri());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","spatial");
            extrasJson.addProperty("value",dcatMetadata.getSpatialCoverage());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","temporal_start");
            extrasJson.addProperty("value",dcatMetadata.getTemporalStart());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","temporal_end");
            extrasJson.addProperty("value",dcatMetadata.getTemporalEnd());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","theme");
            extrasJson.addProperty("value",dcatMetadata.getThemes());
            extrasJsonArray.add(extrasJson);
            extrasJson=new JsonObject();
            extrasJson.addProperty("key","access_rights");
            extrasJson.addProperty("value",dcatMetadata.getDatasetRights());
            extrasJsonArray.add(extrasJson);


            //espacio para tags
            keywords=dcatMetadata.getKeywords();
            for (String tag: keywords){
                tags=new JsonObject();
                //tags.addProperty("vocabulary_id","null");
                tags.addProperty("name",tag);
                tagsJsonArray.add(tags);
            }
            //extrasJsonArray.add(extrasJson);}
            dataJson.add("extras",extrasJsonArray);
            dataJson.add("tags",tagsJsonArray);
        }
        logger.debug("dataJson: {}",dataJson);
        // create the CKAN request URL
        String urlPath = "/api/3/action/package_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String packageId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            logger.info("Successful package creation (pkgName/pkgId=\"{}/{}\")", pkgName, packageId);
            return packageId;
        /*
        This is not deleted if in the future we try to activate deleted elements again

        } else if (res.getStatusCode() == 409) {
            logger.debug("The package exists but its state is \"deleted\", activating it (pkgName="
                    + pkgName + ")");
            String packageId = activateElementState(httpClient, pkgName, "dataset");

            if (packageId != null) {
                logger.debug("Successful package activation (pkgId=" + packageId + ")");
                return packageId;
            } else {
                throw new CygnusRuntimeError("Could not activate the package (pkgId=" + pkgName + ")");
            } // if else
        */
        } else {
            throw new Exception("Could not create the package (orgId=" + orgId
                    + ", pkgName=" + pkgName + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // createPackage

    /**
     * Creates a resource within a given package in CKAN.
     * @param resName Resource to be created
     * @param pkgId Package the resource belongs to
     * @return A resource identifier if the resource was created or an exception if something went wrong
     */
    private String createResource(String resName, String pkgId, DCATMetadata dcatMetadata) throws Exception {
        // create the CKAN request JSON

        JsonArray extrasJsonArray = new JsonArray();
        JsonObject extrasJson = new JsonObject();

        JsonObject dataJson = new JsonObject();
        dataJson.addProperty("package_id",pkgId);
        dataJson.addProperty("name",resName);
        dataJson.addProperty("access_url",dcatMetadata.getAccessURL());
        dataJson.addProperty("format",dcatMetadata.getFormat());
        dataJson.addProperty("availability",dcatMetadata.getAvailability());
        dataJson.addProperty("description",dcatMetadata.getResourceDescription());
        dataJson.addProperty("mimetype",dcatMetadata.getMimeType());
        dataJson.addProperty("license",dcatMetadata.getLicense());
        dataJson.addProperty("download_url",dcatMetadata.getDownloadURL());
        dataJson.addProperty("size",dcatMetadata.getByteSize());
        dataJson.addProperty("url",dcatMetadata.getDownloadURL());
        dataJson.addProperty("rights",dcatMetadata.getResourceRights());
        extrasJson.addProperty("key","license_type");
        extrasJson.addProperty("value",dcatMetadata.getLicenseType());
        //extrasJsonArray.add(extrasJson);
        dataJson.add("extras",extrasJsonArray);
        logger.debug("dataJson: {}",dataJson);
        // create the CKAN request URL
        String urlPath = "/api/3/action/resource_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, dataJson.toString());

        // check the status
        if (res.getStatusCode() == 200) {
            String resourceId = ((JSONObject) res.getJsonObject().get("result")).get("id").toString();
            logger.info("Successful resource creation (resName/resId=\"{}/{}\")", resName, resourceId);
            return resourceId;
        } else {
            throw new Exception("Could not create the resource (pkgId=" + pkgId
                    + ", resName=" + resName + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // createResource

    /**
     * Creates a datastore for a given resource in CKAN.
     * @param resId Identifies the resource whose datastore is going to be created.
     * @param records Array list with the attributes names for being used as fields with column mode
     */
    private void createDataStoreWithFields(String resId, String resName, String records) throws Exception {
        // create the CKAN request JSON
        // CKAN types reference: http://docs.ckan.org/en/ckan-2.2/datastore.html#valid-types
        org.json.JSONObject jsonContent = new org.json.JSONObject(records);
        Iterator<String> keys = jsonContent.keys();
        ArrayList<String> fields = new ArrayList<>();
        Gson gson = new Gson();
        DataStore dataStore = new DataStore();
        ArrayList<JsonElement> jsonArray = new ArrayList<>();

        while (keys.hasNext()) {
            String key = keys.next();
            fields.add(key);
        }

        for (String field : fields) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", field);
            jsonObject.addProperty("type", "text");
            jsonArray.add((jsonObject.getAsJsonObject()));
        }

        dataStore.setResource_id(resId);
        dataStore.setAliases(resName);
        dataStore.setFields(jsonArray);
        dataStore.setForce("true");
        String jsonString = gson.toJson(dataStore);

        logger.info("Successful datastore creation jsonString=\"{}\"", jsonString);

        // create the CKAN request URL
        String urlPath = "/api/3/action/datastore_create";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            logger.info("Successful datastore creation (resourceId=\"{}\")", resId);
        } else {
            throw new Exception("Could not create the datastore (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // createResource

    /**
     * Creates a view for a given resource in CKAN.
     * @param resId Identifies the resource whose view is going to be created.
     */
    private void createView(String resId) throws Exception {
        if (!existsView(resId)) {
            // create the CKAN request JSON
            String jsonString = "{ \"resource_id\": \"" + resId + "\","
                    + "\"view_type\": \"" + viewer + "\","
                    + "\"title\": \"Recline grid view\" }";

            // create the CKAN request URL
            String urlPath = "/api/3/action/resource_view_create";

            // do the CKAN request
            JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

            // check the status
            if (res.getStatusCode() == 200) {
                logger.info("Successful view creation (resourceId=\"{}\")", resId);
            } else {
                throw new Exception("Could not create the datastore (resId=" + resId
                        + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
            } // if else
        } // if
    } // createView

    private boolean existsView(String resId) throws Exception {
        // create the CKAN request JSON
        String jsonString = "{ \"id\": \"" + resId + "\" }";

        // create the CKAN request URL
        String urlPath = "/api/3/action/resource_view_list";

        // do the CKAN request
        JsonResponse res = doCKANRequest("POST", urlPath, jsonString);

        // check the status
        if (res.getStatusCode() == 200) {
            logger.info("Successful view listing (resourceId=\"{}\")", resId);
            return (!((JSONArray) res.getJsonObject().get("result")).isEmpty());
        } else {
            throw new Exception("Could not check if the view exists (resId=" + resId
                    + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // if else
    } // existsView

    private JsonResponse doCKANRequest(String method, String urlPath, String jsonString) throws Exception {
        ArrayList<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Authorization", apiKey));
        headers.add(new BasicHeader("Content-Type", "application/json; charset=utf-8"));
        return doRequest(method, urlPath, true, headers, new StringEntity(jsonString, "UTF-8"));
    } // doCKANRequest


    /**
     * Builds an organization name given an organizationName. It throws an exception if the naming conventions are violated.
     * @return Organization name
     */
    public String buildOrgName(String organizationName, DCATMetadata dcatMetadata) throws Exception {
        String orgName;
        String finalOrganizationName;

        if (dcatMetadata != null && dcatMetadata.getOrganizationName() != null) {
            finalOrganizationName = dcatMetadata.getOrganizationName().toLowerCase(Locale.ENGLISH);
        } else {
            finalOrganizationName = organizationName.toLowerCase(Locale.ENGLISH);
        }

        orgName = NGSICharsets.encodeCKAN(finalOrganizationName);
        int orgNameLength = orgName.length();

        if (orgNameLength > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new Exception("Building organization name '" + orgName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (orgNameLength < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new Exception("Building organization name '" + orgName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if

        return orgName;
    } // buildOrgName

    /**
     * Builds a package name given an entity and a package title. It throws an exception if the naming
     * conventions are violated.
     * @return Package name
     */
    public String buildPkgName(String pkgTitle, DCATMetadata dcatMetadata) throws Exception {
        String pkgName;
        String finalPackageName;

        if (dcatMetadata != null && dcatMetadata.getPackageName() != null) {
            finalPackageName = dcatMetadata.getPackageName().toLowerCase(Locale.ENGLISH);
        } else {
            finalPackageName = pkgTitle.toLowerCase(Locale.ENGLISH);
        }

        pkgName = NGSICharsets.encodeCKAN(finalPackageName);

        if (pkgName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new Exception("Building package name '" + pkgName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (pkgName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new Exception("Building package name '" + pkgName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if

        return pkgName;
    } // buildPkgName

    /**
     * Builds a resource name given an entity. It throws an exception if the naming conventions are violated.
     * @return Resource name
     */
    public String buildResName(Entity entity, DCATMetadata dcatMetadata) throws Exception {
        String resName;

        if (dcatMetadata != null && dcatMetadata.getResourceName() != null) {
            resName = dcatMetadata.getResourceName();
        } else {
            String entityTitle = ngsiUtils.getSpecificAttributeValue(entity, "title");
            resName = entityTitle != null ? entityTitle : entity.getEntityId();
        }

        if (resName.length() > NGSIConstants.CKAN_MAX_NAME_LEN) {
            throw new Exception("Building resource name '" + resName + "' and its length is "
                    + "greater than " + NGSIConstants.CKAN_MAX_NAME_LEN);
        } else if (resName.length() < NGSIConstants.CKAN_MIN_NAME_LEN) {
            throw new Exception("Building resource name '" + resName + "' and its length is "
                    + "lower than " + NGSIConstants.CKAN_MIN_NAME_LEN);
        } // if else if

        return resName;
    } // buildResName
}
