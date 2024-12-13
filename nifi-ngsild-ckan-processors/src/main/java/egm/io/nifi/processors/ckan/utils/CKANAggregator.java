package egm.io.nifi.processors.ckan.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import egm.io.nifi.processors.ckan.ngsild.AttributesLD;
import egm.io.nifi.processors.ckan.ngsild.Entity;
import egm.io.nifi.processors.ckan.ngsild.NGSIConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;


public abstract class CKANAggregator {
    /**
     * The Aggregation of processed entityes.
     */
    protected LinkedHashMap<String, ArrayList<JsonElement>> aggregation;

    // string containing the data fieldValues
    protected String service;
    protected String entityForNaming;
    protected String entityTypeForNaming;
    protected String attributeForNaming;
    protected String attribute;
    protected String orgName;
    protected String pkgName;
    protected String resName;
    private static final Logger logger = LoggerFactory.getLogger(CKANAggregator.class);


    /**
     * Gets aggregation.
     *
     * @return the aggregation
     */
    public LinkedHashMap<String, ArrayList<JsonElement>> getAggregation() {
        if (aggregation == null) {
            return new LinkedHashMap<>();
        } else {
            return aggregation;
        }
    } //getAggregation

    /**
     * Gets aggregation to persist. This means that the returned aggregation will not have metadata
     * in case that attrMetadataStore is set to false. Also, added fields for processing purposes
     * will be removed from the aggregation (like attrType on Column mode).
     *
     * @return the aggregation to persist
     */
    public LinkedHashMap<String, ArrayList<JsonElement>> getAggregationToPersist() {
        if (aggregation == null) {
            return new LinkedHashMap<>();
        } else {
            return linkedHashMapWithoutDefaultFields(aggregation);
        }
    } //getAggregationToPersist

    /**
     * Sets aggregation.
     *
     * @param aggregation the aggregation
     */
    public void setAggregation(LinkedHashMap<String, ArrayList<JsonElement>> aggregation) {
        this.aggregation = aggregation;
    } //setAggregation

    public void setService(String service) {
        this.service = service;
    }

    public void setEntityForNaming(String entityForNaming) {
        this.entityForNaming = entityForNaming;
    }

    public void setEntityTypeForNaming(String entityTypeForNaming) {
        this.entityTypeForNaming = entityTypeForNaming;
    }
    /**
     * Gets attribute.
     *
     * @return the attribute
     */
    public String getAttribute() {
        return attribute;
    } //getAttribute

    /**
     * Sets attribute.
     *
     * @param attribute the attribute
     */
    public void setAttribute(String attribute) {
        this.attribute = attribute;
    } //setAttribute

    public void setAttributeForNaming(String attributeForNaming) {
        this.attributeForNaming = attributeForNaming;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public void setPkgName(String pkgName) {
        this.pkgName = pkgName;
    }

    public void setResName(String resName) {
        this.resName = resName;
    }

    protected String tableName;
    protected String typedFieldNames;
    protected String fieldNames;


    public String getOrgName() {
        return orgName.toLowerCase();
    }

    public String getPkgName() {
            return pkgName.toLowerCase();
    }

    public String getResName() {
            return resName.toLowerCase();
    }

    public String getTableName() {
            return tableName.toLowerCase();
    } // getTableName

    public String getTypedFieldNames() {
        return typedFieldNames;
    } // getTypedFieldNames

    public String getFieldNames() {
        return fieldNames;
    } // getFieldNames

    public void initialize(Entity entity) throws Exception {
        entityForNaming = entity.getEntityId();
        entityTypeForNaming=entity.getEntityType();
    } // initialize

    public abstract void aggregate(Entity entity, long creationTime);

    /**
     * Class for aggregating batches in row mode.
     */
    public class RowAggregator extends CKANAggregator {
        @Override
        public void initialize(Entity entity) throws Exception {
            super.initialize(entity);
            typedFieldNames = "("
                    + NGSIConstants.RECV_TIME_TS + " bigint,"
                    + NGSIConstants.RECV_TIME + " text,"
                    + NGSIConstants.FIWARE_SERVICE_PATH + " text,"
                    + NGSIConstants.ENTITY_ID + " text,"
                    + NGSIConstants.ENTITY_TYPE + " text,"
                    + NGSIConstants.ATTR_NAME + " text,"
                    + NGSIConstants.ATTR_TYPE + " text,"
                    + NGSIConstants.ATTR_VALUE + " text,"
                    + NGSIConstants.ATTR_MD + " text"
                    + ")";
            fieldNames = "("
                    + NGSIConstants.RECV_TIME_TS + ","
                    + NGSIConstants.RECV_TIME + ","
                    + NGSIConstants.FIWARE_SERVICE_PATH + ","
                    + NGSIConstants.ENTITY_ID + ","
                    + NGSIConstants.ENTITY_TYPE + ","
                    + NGSIConstants.ATTR_NAME + ","
                    + NGSIConstants.ATTR_TYPE + ","
                    + NGSIConstants.ATTR_VALUE + ","
                    + NGSIConstants.ATTR_MD
                    + ")";
        }

        @Override
        public void aggregate(Entity entity, long creationTime) {

        }

    }// RowAggregator


    /**
     * Class for aggregating batches in column mode.
     */
    public class ColumnAggregator extends CKANAggregator {
        @Override
        public void initialize(Entity entity) throws Exception {
            super.initialize(entity);
            LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();

            aggregation.put(NGSIConstants.RECV_TIME, new ArrayList<>());
            aggregation.put(NGSIConstants.ENTITY_ID, new ArrayList<>());
            aggregation.put(NGSIConstants.ENTITY_TYPE, new ArrayList<>());

            // iterate on all this context element attributes, if there are attributes
            ArrayList<AttributesLD> attributes = entity.getEntityAttrsLD();

            if (attributes == null || attributes.isEmpty()) {
                return;
            } // if

            // iterate on all this context element attributes, if there are attributes
            for (AttributesLD attribute : attributes) {
                String attrName = attribute.getAttrName();
                String subAttrName;
                aggregation.put(attrName, new ArrayList<>());
                aggregation.put(attrName + NGSIConstants.AUTOGENERATED_ATTR_TYPE, new ArrayList<>());
                if (attribute.isHasSubAttrs()){
                    for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                        subAttrName = subAttribute.getAttrName();
                        aggregation.put(attrName+ "_" + subAttrName, new ArrayList<>());
                        aggregation.put(attrName + "_" + subAttrName+ NGSIConstants.AUTOGENERATED_ATTR_TYPE, new ArrayList<>());
                    }
                }
            }
            setAggregation(aggregation);
        } // initialize

        @Override
        public void aggregate(Entity entity, long creationTime) {
            LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();

            // get the getRecvTimeTs headers
            String recvTime = CommonConstants.getHumanReadable(creationTime, true);

            // get the getRecvTimeTs body
            String entityId = entity.getEntityId();
            String entityType = entity.getEntityType();

            // iterate on all this context element attributes, if there are attributes
            // iterate on all this context element attributes, if there are attributes
            ArrayList<AttributesLD> attributes = entity.getEntityAttrsLD();

            if (attributes == null || attributes.isEmpty()) {
                logger.info("No attributes within the notified entity, nothing is done (id=\"{}\", type=\"{}\")", entityId, entityType);
                return;
            } // if

            logger.info("Data to be aggregated: {}", entity);

            aggregation.get(NGSIConstants.RECV_TIME).add(new JsonPrimitive(recvTime));
            aggregation.get(NGSIConstants.ENTITY_ID).add(new JsonPrimitive(entityId));
            aggregation.get(NGSIConstants.ENTITY_TYPE).add(new JsonPrimitive(entityType));

            for (AttributesLD attribute : attributes) {
                String attrName = attribute.getAttrName();
                String attrType = attribute.getAttrType();
                JsonElement attrValue = new JsonPrimitive(attribute.getAttrValue());
                String subAttrName =  "";
                JsonElement subAttrValue;
                if (aggregation.containsKey(attrName)) {
                    aggregation.get(attrName).add(attrValue);
                    aggregation.get(attrName + NGSIConstants.AUTOGENERATED_ATTR_TYPE).add(new JsonPrimitive(attrType));
                }
                if (attribute.isHasSubAttrs()){
                    for (AttributesLD subAttribute : attribute.getSubAttrs()) {
                        if (aggregation.containsKey(attrName+"_"+subAttrName)) {
                            subAttrValue=new JsonPrimitive(subAttribute.getAttrValue());
                            subAttrName= subAttribute.getAttrName();
                            aggregation.get(attrName + "_" + subAttrName).add(subAttrValue);
                            aggregation.get(attrName + "_" + subAttrName + NGSIConstants.AUTOGENERATED_ATTR_TYPE).add(new JsonPrimitive(subAttribute.getAttrType()));
                        }
                    }
                }
            }// for

            logger.info("Aggregated data: {}", aggregation);

            setAggregation(aggregation);

        } // aggregate

    } // ColumnAggregator

    public CKANAggregator getAggregator(boolean rowAttrPersistence) {
        if (rowAttrPersistence) {
            return new RowAggregator();
        } else {
            return new ColumnAggregator();
        } // if else
    } // getAggregator

    /**
     * Linked hash map without default fields linked hash map.
     *
     * @param aggregation       the aggregation
     * @return the linked hash map without metadata objects (if attrMetadataStore is set to true)
     * also, removes "_type" and "RECV_TIME_TSC" keys from the object
     */
    public static LinkedHashMap<String, ArrayList<JsonElement>> linkedHashMapWithoutDefaultFields(LinkedHashMap<String, ArrayList<JsonElement>> aggregation) {
        ArrayList<String> keysToCrop = new ArrayList<>();
        for (String entry : aggregation.keySet()) {
            if ((entry.equals(NGSIConstants.RECV_TIME_TS + "C") || entry.contains(NGSIConstants.AUTOGENERATED_ATTR_TYPE))) {
                keysToCrop.add(entry);
            }
        }
        return cropLinkedHashMap(aggregation, keysToCrop);
    }

    /**
     * Linked hash map to json list with out empty md array list.
     *
     * @param aggregation the aggregation
     * @return an ArrayList of JsonObjects wich contain all attributes on a LinkedHashMap, this method also removes empty medatada fields.
     */
    public static ArrayList<JsonObject> linkedHashMapToJson(LinkedHashMap<String, ArrayList<JsonElement>> aggregation) {
        ArrayList<JsonObject> jsonStrings = new ArrayList<>();
        int numEvents = collectionSizeOnLinkedHashMap(aggregation);
        for (int i = 0; i < numEvents; i++) {
            Iterator<String> it = aggregation.keySet().iterator();
            JsonObject jsonObject = new JsonObject();
            while (it.hasNext()) {
                String entry = it.next();
                ArrayList<JsonElement> values = aggregation.get(entry);
                if (values.get(i) != null) {
                    jsonObject.add(entry, values.get(i));
                }
            }
            jsonStrings.add(jsonObject);
        }
        return jsonStrings;
    }

    /**
     * Collection size on linked hash map int.
     *
     * @param aggregation the aggregation
     * @return the number of attributes contained on the aggregation object.
     */
    public static int collectionSizeOnLinkedHashMap(LinkedHashMap<String, ArrayList<JsonElement>> aggregation) {
        ArrayList<ArrayList<JsonElement>> list = new ArrayList<>(aggregation.values());
        return list.get(0).size();
    }

    /**
     * Crop linked hash map linked hash map.
     *
     * @param aggregation the aggregation
     * @param keysToCrop  the keys to crop
     * @return removes all keys on list keysToCrop from the aggregation object.
     */
    public static LinkedHashMap<String, ArrayList<JsonElement>> cropLinkedHashMap(LinkedHashMap<String, ArrayList<JsonElement>> aggregation, ArrayList<String> keysToCrop) {
        LinkedHashMap<String, ArrayList<JsonElement>> cropedLinkedHashMap = (LinkedHashMap<String, ArrayList<JsonElement>>) aggregation.clone();
        for (String key : keysToCrop) {
            cropedLinkedHashMap.remove(key);
        }
        return cropedLinkedHashMap;
    }



} // CKANAggregator
