
package egm.io.nifi.processors.ckan.utils;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import egm.io.nifi.processors.ckan.http.HttpBackend;
import egm.io.nifi.processors.ckan.http.JsonResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author anmunozx
 */
public class CKANCache extends HttpBackend {
    
    private final String apiKey;
    private final HashMap<String, HashMap<String, ArrayList<String>>> tree; // this cache only contain human readable names
    private final HashMap<String, String> orgMap; // this cache contains the translation from organization name to identifier
    private final HashMap<String, String> pkgMap; // this cache contains the translation from package name to identifier
    private final HashMap<String, String> resMap; // this cache contains the translation from resource name to identifier
    private static final Logger logger = LoggerFactory.getLogger(CKANCache.class);

    public CKANCache(String[] host, String port, boolean ssl, String apiKey) {
        super(host, port, ssl);
        this.apiKey = apiKey;
        tree = new HashMap<>();
        orgMap = new HashMap<>();
        pkgMap = new HashMap<>();
        resMap = new HashMap<>();
    } // CKANCache
    
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
     * @param orgName Organization name
     * @param orgId Organization id
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
     * @param orgName Organization name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedOrg(String orgName) throws Exception{
        // check if the organization has already been cached
        if (tree.containsKey(orgName)) {
            logger.info("Organization found in the cache (orgName=\"{}\")", orgName);
            return true;
        } // if

        logger.info("Organization not found in the cache, querying CKAN for it (orgName=\"{}\")", orgName);
        
        // query CKAN for the organization information
        String ckanURL = "/api/3/action/organization_autocomplete?q=" + orgName;

        ArrayList<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Authorization", apiKey));
        JsonResponse res = doRequest("GET", ckanURL, true, headers, null);

        switch (res.getStatusCode()) {
            case 200:
                // the organization exists in CKAN
                JSONArray result = (JSONArray) res.getJsonObject().get("result");
                JSONObject organization = (JSONObject) result.get(0);
                
                // put the organization in the tree and in the organization map
                String orgId = organization.get("id").toString();
                tree.put(orgName, new HashMap<>());
                orgMap.put(orgName, orgId);
                logger.info("Organization found in CKAN, now cached (orgName/orgId=\"{}/{}\")", orgName, orgId);
                return true;
            case 404:
                return false;
            default:
                throw new Exception("Could not check if the organization exists ("
                        + "orgName=" + orgName + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // switch
    } // isCachedOrg
    
    /**
     * Checks if the package is cached. If not cached, CKAN is queried in order to update the cache.
     * This method assumes the given organization exists and it is cached.
     * @param orgName Organization name
     * @param pkgName Package name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedPkg(String orgName, String pkgName) throws Exception {
        // check if the package has already been cached
        if (tree.get(orgName).containsKey(pkgName)) {
            logger.info("Package found in the cache (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);
            return true;
        } // if

        logger.info("Package not found in the cache, querying CKAN for it (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);

        // query CKAN for the package information
        String ckanURL = "/api/3/action/package_show?id=" + pkgName;
        ArrayList<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Authorization", apiKey));
        JsonResponse res = doRequest("GET", ckanURL, true, headers, null);

        switch (res.getStatusCode()) {
            case 200:
                // the package exists in CKAN
                JSONObject result = (JSONObject) res.getJsonObject().get("result");
                
                // check if the package is in "deleted" state
                String pkgState = result.get("state").toString();
                
                if (pkgState.equals("deleted")) {
                    throw new Exception("The package '" + pkgName + "' exists but it is in a "
                            + "deleted state");
                } // if
                
                // put the package in the tree and in the package map
                String pkgId = result.get("id").toString();
                tree.get(orgName).put(pkgName, new ArrayList<>());
                orgMap.put(pkgName, pkgId);
                logger.info("Package found in CKAN, now cached (orgName=\"{}\", pkgName/pkgId=\"{}/{}\")", orgName, pkgName, pkgId);
                
                // get the resource and populate the resource map
                JSONArray resources = (JSONArray) result.get("resources");
                logger.info("Going to populate the resources cache (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);
                populateResourcesMap(resources, orgName, pkgName, false);
                return true;
            case 404:
                return false;
            default:
                throw new Exception("Could not check if the package exists ("
                        + "orgName=" + orgName + ", pkgName=" + pkgName + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // switch
    } // isCachedPkg
    
    /**
     * Checks if the resource is cached. If not cached, CKAN is queried in order to update the cache.
     * This method assumes the given organization and package exist and they are cached.
     * @param orgName Organization name
     * @param pkgName Package name
     * @param resName Resource name
     * @return True if the organization was cached, false otherwise
     */
    public boolean isCachedRes(String orgName, String pkgName, String resName)
        throws Exception {
        // check if the resource has already been cached
        if (tree.get(orgName).get(pkgName).contains(resName)) {
            logger.info("Resource found in the cache (orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);
            return true;
        } // if

        logger.info("Resource not found in the cache, querying CKAN for the whole package containing it "
                + "(orgName=\"{}\", pkgName=\"{}\", resName=\"{}\")", orgName, pkgName, resName);
        
        // reached this point, we need to query CKAN about the resource, in order to know if it exists in CKAN
        // nevertheless, the CKAN API allows us to query for a certain resource by id, not by name...
        // the only solution seems to query for the whole package and check again
        // query CKAN for the organization information
        
        String ckanURL = "/api/3/action/package_show?id=" + pkgName;
        ArrayList<Header> headers = new ArrayList<>();
        headers.add(new BasicHeader("Authorization", apiKey));
        JsonResponse res = doRequest("GET", ckanURL, true, headers, null);

        switch (res.getStatusCode()) {
            case 200:
                // the package exists in CKAN
                logger.info("Package found in CKAN, going to update the cached resources (orgName=\"{}\", pkgName=\"{}\")", orgName, pkgName);
                
                // there is no need to check if the package is in "deleted" state...
                
                // there is no need to put the package in the tree nor put it in the package map...
                
                // get the resource and populate the resource map
                JSONObject result = (JSONObject) res.getJsonObject().get("result");
                JSONArray resources = (JSONArray) result.get("resources");
                
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
                    } // if else
                } // if else
            case 404:
                return false;
            default:
                throw new Exception("Could not check if the resource exists ("
                        + "orgName=" + orgName + ", pkgName=" + pkgName + ", resName=" + resName
                        + ", statusCode=" + res.getStatusCode() + ", response=" + res.getJsonObject().toString() + ")");
        } // switch
    } // isCachedRes

    /**
     * Populates the resourceName-resource map of a given orgName with the package information from the CKAN response.
     * @param resources JSON vector from the CKAN response containing resource information
     * @param orgName Organization name
     * @param pkgName Package name
     * @param checkExistence If true, checks if the queried resource already exists in the cache
     */
    private void populateResourcesMap(JSONArray resources, String orgName, String pkgName, boolean checkExistence) {
        // this check is for debuging purposes
        if (resources == null || resources.isEmpty()) {
            logger.info("The resources list is empty, nothing to cache");
            return;
        } // if

        logger.info("Resources to be populated: \"{}\" (orgName=\"{}\", pkgName=\"{}\")", resources, orgName, pkgName);
        
        // iterate on the resources
        Iterator<JSONObject> iterator = resources.iterator();
        
        while (iterator.hasNext()) {
            // get the resource name and id (resources cannot be in deleted state)
            JSONObject factObj = iterator.next();
            String resourceName = (String) factObj.get("name");
            String resourceId = (String) factObj.get("id");

            // put the resource in the tree and in the resource map
            if (checkExistence) {
                if (tree.get(orgName).get(pkgName).contains(resourceName)) {
                    continue;
                } // if
            } // if
            
            tree.get(orgName).get(pkgName).add(resourceName);
            this.setResId(orgName, pkgName, resourceName, resourceId);
            logger.info("Resource found in CKAN, now cached (orgName=\"{}\" -> pkgName=\"{}\" -> " +
                    "resourceName/resourceId=\"{}/{}\")", orgName, pkgName, resourceName, resourceId);
        } // while
    } // populateResourcesMap
} // CKANCache
