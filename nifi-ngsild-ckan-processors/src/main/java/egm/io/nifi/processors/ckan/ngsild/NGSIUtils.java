package egm.io.nifi.processors.ckan.ngsild;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static egm.io.nifi.processors.ckan.ngsild.NGSIConstants.*;


public class NGSIUtils {

    public static List<String> IGNORED_KEYS_ON_ATTRIBUTES =
            List.of(NGSILD_TYPE, NGSILD_VALUE, NGSILD_OBJECT, NGSILD_CREATED_AT, NGSILD_MODIFIED_AT);
    private static final Logger logger = LoggerFactory.getLogger(NGSIUtils.class);

    public NGSIEvent getEventFromFlowFile(FlowFile flowFile, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        final Logger logger = LoggerFactory.getLogger(NGSIUtils.class);

        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        // Create the PreparedStatement to use for this FlowFile.
        Map<String, String> flowFileAttributes = flowFile.getAttributes();
        Map <String,String> newFlowFileAttributes = new CaseInsensitiveMap(flowFileAttributes);
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);
        String fiwareService = (newFlowFileAttributes.get("fiware-service") == null) ? "nd":newFlowFileAttributes.get("fiware-service");
        String fiwareServicePath = (newFlowFileAttributes.get("fiware-servicepath")==null) ? "/nd":newFlowFileAttributes.get("fiware-servicepath");
        System.out.println(fiwareServicePath);
        long creationTime=flowFile.getEntryDate();
        JSONObject content = new JSONObject(flowFileContent);
        JSONArray data;
        String entityType;
        String entityId;
        ArrayList<Entity> entities = new ArrayList<>();
        NGSIEvent event= null;

        System.out.println("NGSI-LD Notification");
        boolean hasSubAttrs= false;
        data = (JSONArray) content.get(NGSILD_DATA);
        for (int i = 0; i < data.length(); i++) {
            JSONObject lData = data.getJSONObject(i);
            entityId = lData.getString(NGSILD_ID);
            entityType = lData.getString(NGSILD_TYPE);
            ArrayList<AttributesLD> attributes  = new ArrayList<>();
            Iterator<String> keys = lData.keys();
            String attrType;
            String attrValue="";
            String subAttrName;
            String subAttrType;
            String subAttrValue="";
            ArrayList<AttributesLD> subAttributes=new ArrayList<>();

            while (keys.hasNext()) {
                String key = keys.next();
                if (!NGSILD_ID.equals(key) && !NGSILD_TYPE.equals(key) && !NGSILD_CONTEXT.equals(key)){
                    JSONObject value = lData.getJSONObject(key);
                    attrType = value.getString(NGSILD_TYPE);
                    if (NGSILD_RELATIONSHIP.contentEquals(attrType)){
                        attrValue = value.get(NGSILD_OBJECT).toString();
                    }else if (NGSILD_PROPERTY.contentEquals(attrType)){
                        attrValue = value.get(NGSILD_VALUE).toString();

                    }else if (NGSILD_GEOPROPERTY.contentEquals(attrType)){
                        attrValue = value.get(NGSILD_VALUE).toString();
                    }
                    Iterator<String> keysOneLevel = value.keys();
                    while (keysOneLevel.hasNext()) {
                        String keyOne = keysOneLevel.next();
                        if (IGNORED_KEYS_ON_ATTRIBUTES.contains(keyOne)){
                            // Do Nothing
                        } else if (keyOne.equals(NGSILD_OBSERVED_AT ) || keyOne.equals(NGSILD_UNIT_CODE)){
                            // Leave the value as it is
                            String value2 = value.getString(keyOne);
                            subAttrName = keyOne;
                            subAttrValue = value2;
                            hasSubAttrs = true;

                            subAttributes.add(new AttributesLD(subAttrName,subAttrValue,subAttrValue,false,null));
                        } else {
                            JSONObject value2 = value.getJSONObject(keyOne);
                            subAttrName=keyOne;
                            subAttrType=value2.get(NGSILD_TYPE).toString();
                            if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)){
                                subAttrValue = value2.get(NGSILD_OBJECT).toString();
                            }else if (NGSILD_PROPERTY.contentEquals(subAttrType)){
                                subAttrValue = value2.get(NGSILD_VALUE).toString();
                            }else if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)){
                                subAttrValue = value2.get(NGSILD_VALUE).toString();
                            } else if ("RelationshipDetails".contains(keyOne)) {
                                value2.remove(NGSILD_ID);
                                value2.remove(NGSILD_TYPE);

                                for (String relationKey : value2.keySet()) {
                                    Object object = value2.get(relationKey);
                                    if (object instanceof JSONArray) {
                                        // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                                        JSONArray valuesArray = value2.getJSONArray(relationKey);
                                        for (int j = 0; j < valuesArray.length(); j++) {
                                            JSONObject valueObject = valuesArray.getJSONObject(j);
                                            AttributesLD subAttribute = parseNgsiLdSubAttribute(relationKey, valueObject);
                                            addAttributeIfValid(subAttributes, subAttribute);
                                        }
                                    } else if (object instanceof JSONObject) {
                                        AttributesLD subAttribute = parseNgsiLdSubAttribute(relationKey, (JSONObject) object);
                                        addAttributeIfValid(subAttributes, subAttribute);
                                    } else {
                                        logger.info("Sub Attribute {} has unexpected value type: {}", relationKey, object.getClass());
                                    }
                                }
                            }
                            hasSubAttrs= true;
                            subAttributes.add(new AttributesLD(subAttrName,subAttrType,subAttrValue,false,null));
                        }
                    }
                    attributes.add(new AttributesLD(key,attrType,attrValue, hasSubAttrs,subAttributes));
                    subAttributes=new ArrayList<>();
                    hasSubAttrs= false;
                }
            }
            entities.add(new Entity(entityId,entityType,attributes));
        }
        event = new NGSIEvent(creationTime,fiwareService,entities);
        return event;
    }

    private AttributesLD parseNgsiLdSubAttribute(String key, JSONObject value) {
        String subAttrType = value.get(NGSILD_TYPE).toString();
        String subAttrValue = "";
        if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_OBJECT).toString();
        } else if (NGSILD_PROPERTY.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_VALUE).toString();
        } else if (NGSILD_GEOPROPERTY.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_VALUE).toString();
        }

        return new AttributesLD(key.toLowerCase(), subAttrType, subAttrValue, false, null);
    }

    // When this processor is used in a flow with a `Join Enrichment` processor, it harmonizes JSON among all processed entities,
    // for instance adding attributes which are not present by default in an entity.
    // In this case, these attributes are null or can have a null value.
    // So we filter out attributes that contain a null value or whose whole value is null
    private void addAttributeIfValid(List<AttributesLD> attributes, AttributesLD attribute) {
        if (attribute != null &&
                attribute.getAttrValue() != null &&
                !Objects.equals(attribute.getAttrValue(), "null"))
            attributes.add(attribute);
    }

    public String getSpecificAttributeValue(Entity entity, String attributeName) {
        ArrayList<AttributesLD> entityAttributes = entity.getEntityAttrsLD();
        for(AttributesLD attr : entityAttributes) {
            if(attr.getAttrName().toLowerCase().equals(attributeName.toLowerCase())) {
                return  attr.getAttrValue();
            }
        }
        logger.info("Did not find attribute " + attributeName + " in entity " + entity.getEntityId());

        return null;

    }
}
