package egm.io.nifi.processors.ckan.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import egm.io.nifi.processors.ckan.http.HttpBackend;
import egm.io.nifi.processors.ckan.http.JsonResponse;
import okhttp3.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author anmunozx
 */
public class CKANCache extends HttpBackend {

    private static final Logger logger = LoggerFactory.getLogger(CKANCache.class);
    private final String apiKey;
    private final HashMap<String, HashMap<String, ArrayList<String>>> tree; // this cache only contain human readable names
    private final HashMap<String, String> orgMap; // this cache contains the translation from organization name to identifier
    private final HashMap<String, String> pkgMap; // this cache contains the translation from package name to identifier
    private final HashMap<String, String> resMap; // this cache contains the translation from resource name to identifier

    public CKANCache(String url, String apiKey) {
        super(url);
        this.apiKey = apiKey;
        tree = new HashMap<>();
        orgMap = new HashMap<>();
        pkgMap = new HashMap<>();
        resMap = new HashMap<>();
    }

    /**
     * Gets the organization id, given its name.
     */
    public String getOrgId(String orgName) {
        return orgMap.get(orgName);
    } // getOrgId

    /**
     * Gets the package id, given its name.
     */
    public String getPkgId(String orgName, String pkgName) {
        return pkgMap.get(orgName + "_" + pkgName);
    } // getPkgId

    /**
     * Gets the resource id, given its name.
     */
    public String getResId(String orgName, String pkgName, String resName) {
        return resMap.get(orgName + "_" + pkgName + "_" + resName);
    } // getResId

    /**
     * Sets the organization id, given its name.
     *
     * @param orgName Organization name
     * @param orgId   Organization id
     */
    public void setOrgId(String orgName, String orgId) {
        orgMap.put(orgName, orgId);
    } // setOrgId

    /**
     * Sets the package id, given its name.
     */
    public void setPkgId(String orgName, String pkgName, String pkgId) {
        pkgMap.put(orgName + "_" + pkgName, pkgId);
    } // setPkgId

    /**
     * Sets the resource id, given its name.
     */
    public void setResId(String orgName, String pkgName, String resName, String resId) {
        resMap.put(orgName + "_" + pkgName + "_" + resName, resId);
    } // setResId

    /**
     * Adds an organization to the tree.
     */
    public void addOrg(String orgName) {
        tree.put(orgName, new HashMap<>());
    } // addOrg

    /**
     * Adds a package to the tree within a given package.
     */
    public void addPkg(String orgName, String pkgName) {
        tree.get(orgName).put(pkgName, new ArrayList<>());
    } // addPkg

    /**
     * Adds a resource to the tree within a given package within a given organization.
     */
    public void addRes(String orgName, String pkgName, String resName) {
        tree.get(orgName).get(pkgName).add(resName);
    } // addRes

    /**
     * Checks if the organization is cached. If not cached, CKAN is queried in order to update the cache.
     *
     * @param orgName Organization name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedOrg(String orgName) throws Exception {
        // check if the organization has already been cached
        if (tree.containsKey(orgName)) {
            logger.info("Organization found in the cache (orgName=\"{}\")", orgName);
            return true;
        } // if

        logger.info("Organization not found in the cache, querying CKAN for it (orgName=\"{}\")", orgName);

        // query CKAN for the organization information
        String ckanURL = "/api/3/action/organization_show?id=" + orgName;

        Headers headers = new Headers.Builder().add("Authorization", apiKey).build();
        JsonResponse res = doRequest("GET", ckanURL, headers, null);
        int status = res.statusCode();

        switch (status) {
            case 200:
                // the organization exists in CKAN
                JsonObject result = res.jsonObject().getAsJsonObject("result");

                // put the organization in the tree and in the organization map
                String orgId = result.get("id").getAsString();
                tree.put(orgName, new HashMap<>());
                orgMap.put(orgName, orgId);
                logger.info("Organization found in CKAN, now cached (orgName/orgId=\"{}/{}\")", orgName, orgId);
                return true;
            case 404:
                logger.info("Organization '{}' not found in CKAN", orgName);
                return false;
            default:
                throw new Exception("Could not check if the organization exists ("
                    + "orgName=" + orgName + ", statusCode=" + res.statusCode() + ", response=" + res.jsonObject() + ")");
        }
    }

    /**
     * Checks if the package is cached. If not cached, CKAN is queried in order to update the cache.
     * This method assumes the given organization exists and it is cached.
     *
     * @param orgName Organization name
     * @param pkgName Package name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedPkg(String orgName, String pkgName) throws Exception {
        if (tree.get(orgName).containsKey(pkgName)) {
            logger.info("Package found in the cache (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);
            return true;
        }

        logger.info("Package not found in the cache, querying CKAN for it (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);

        // query CKAN for the package information
        String ckanURL = "/api/3/action/package_search?q=" + pkgName +
            "&fq=organization:" + orgName + "+name:" + pkgName;
        Headers headers = new Headers.Builder().add("Authorization", apiKey).build();
        JsonResponse res = doRequest("GET", ckanURL, headers, null);

        switch (res.statusCode()) {
            case 200:
                JsonObject result = (JsonObject) res.jsonObject().get("result");
                if (result.get("count").getAsLong() == 0L) {
                    logger.info("Package '{}' not found in CKAN", pkgName);
                    return false;
                }

                JsonArray results = result.getAsJsonArray("results");
                JsonObject pkgObject = results.get(0).getAsJsonObject();
                
                // check if the package is in "deleted" state
                String pkgState = pkgObject.get("state").getAsString();

                if (pkgState.equals("deleted")) {
                    throw new Exception("The package '" + pkgName + "' exists but it is in a "
                            + "deleted state");
                }

                // put the package in the tree and in the package map
                String pkgId = pkgObject.get("id").getAsString();
                tree.get(orgName).put(pkgName, new ArrayList<>());
                setPkgId(orgName, pkgName, pkgId);
                logger.info("Package found in CKAN, now cached (orgName={}, pkgName/pkgId={}/{})", orgName, pkgName, pkgId);

                // get the resource and populate the resource map
                JsonArray resources = pkgObject.getAsJsonArray("resources");
                logger.info("Going to populate the resources cache (orgName={}, pkgName={})", orgName, pkgName);
                populateResourcesMap(resources, orgName, pkgName, false);
                return true;
            case 404:
                return false;
            default:
                throw new Exception("Could not check if the package exists ("
                    + "orgName=" + orgName + ", pkgName=" + pkgName + ", statusCode=" + res.statusCode() + ", response=" + res.jsonObject() + ")");
        }
    }

    /**
     * Checks if the resource is cached. If not cached, CKAN is queried in order to update the cache.
     * This method assumes the given organization and package exist and they are cached.
     *
     * @param orgName Organization name
     * @param pkgName Package name
     * @param resName Resource name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedRes(String orgName, String pkgName, String resName) throws Exception {
        if (tree.get(orgName).get(pkgName).contains(resName)) {
            logger.info("Resource found in the cache (orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);
            return true;
        }

        logger.info("Resource not found in the cache, querying CKAN for the whole package containing it "
                + "(orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);

        // Reached this point, we need to query CKAN about the resource to know if it exists in CKAN.
        // The CKAN API allows us to query for a certain resource by id, not by name...
        // The only solution seems to query for the whole package and check again.

        String ckanURL = "/api/3/action/package_show?id=" + pkgName;
        Headers headers = new Headers.Builder().add("Authorization", apiKey).build();
        JsonResponse res = doRequest("GET", ckanURL, headers, null);

        switch (res.statusCode()) {
            case 200:
                // the package exists in CKAN
                logger.info("Package found in CKAN (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);

                // there is no need to check if the package is in "deleted" state...

                // get the resource and populate the resource map
                JsonObject result = res.jsonObject().getAsJsonObject("result");
                JsonArray resources = result.getAsJsonArray("resources");

                if (resources.isEmpty()) {
                    return false;
                } else {
                    logger.info("Going to populate the resources cache (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);
                    populateResourcesMap(resources, orgName, pkgName, true);

                    // check if the resource is within the resources cache, once populated
                    if (tree.get(orgName).get(pkgName).contains(resName)) {
                        logger.info("Resource found in the cache, once queried CKAN " +
                                "(orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);
                        return true;
                    } else {
                        logger.info("Resource not found in the cache, once queried CKAN " +
                                "(orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);
                        return false;
                    }
                }
            case 404:
                return false;
            default:
                throw new Exception("Could not check if the resource exists ("
                    + "orgName=" + orgName + ", pkgName=" + pkgName + ", resName=" + resName
                    + ", statusCode=" + res.statusCode() + ", response=" + res.jsonObject() + ")");
        }
    }

    /**
     * Populates the resourceName-resource map of a given orgName with the package information from the CKAN response.
     *
     * @param resources      JSON vector from the CKAN response containing resource information
     * @param orgName        Organization name
     * @param pkgName        Package name
     * @param checkExistence If true, checks if the queried resource already exists in the cache
     */
    private void populateResourcesMap(JsonArray resources, String orgName, String pkgName, boolean checkExistence) {
        if (resources == null || resources.isEmpty()) {
            logger.info("The resources list is empty, nothing to cache");
            return;
        }

        logger.info("Resources to be populated: {} (orgName={}, pkgName={})", resources, orgName, pkgName);

        for (int i = 0; i < resources.size(); i++) {
            // get the resource name and id (resources cannot be in deleted state)
            JsonObject resourceObject = resources.get(i).getAsJsonObject();
            String resourceName = resourceObject.get("name").getAsString();
            String resourceId = resourceObject.get("id").getAsString();

            // put the resource in the tree and in the resource map
            if (checkExistence) {
                if (tree.get(orgName).get(pkgName).contains(resourceName)) {
                    continue;
                }
            }

            tree.get(orgName).get(pkgName).add(resourceName);
            this.setResId(orgName, pkgName, resourceName, resourceId);
            logger.info("Resource found in CKAN, now cached (orgName={}, pkgName={}, " +
                "resourceName/resourceId={}/{})", orgName, pkgName, resourceName, resourceId);
        }
    }
}
