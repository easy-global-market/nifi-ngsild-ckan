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


public class NGSIUtils {

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
        data = (JSONArray) content.get("data");
        for (int i = 0; i < data.length(); i++) {
            JSONObject lData = data.getJSONObject(i);
            entityId = lData.getString("id");
            entityType = lData.getString("type");
            ArrayList<AttributesLD> attributes  = new ArrayList<>();
            Iterator<String> keys = lData.keys();
            String attrType="";
            String attrValue="";
            String subAttrName="";
            String subAttrType="";
            String subAttrValue="";
            ArrayList<AttributesLD> subAttributes=new ArrayList<>();

            while (keys.hasNext()) {
                String key = keys.next();
                if (!"id".equals(key) && !"type".equals(key) && !"@context".equals(key)){
                    JSONObject value = lData.getJSONObject(key);
                    attrType = value.getString("type");
                    if ("Relationship".contentEquals(attrType)){
                        attrValue = value.get("object").toString();
                    }else if ("Property".contentEquals(attrType)){
                        attrValue = value.get("value").toString();

                    }else if ("GeoProperty".contentEquals(attrType)){
                        attrValue = value.get("value").toString();
                    }
                    Iterator<String> keysOneLevel = value.keys();
                    while (keysOneLevel.hasNext()) {
                        String keyOne = keysOneLevel.next();
                        if ("type".equals(keyOne)){
                            // Do Nothing
                        } else if ("observedAt".equals(keyOne) || "unitCode".equals(keyOne)){
                            // TBD Do Something for unitCode and observedAt
                            String value2 = value.getString(keyOne);
                            subAttrName = keyOne;
                            subAttrValue = value2;
                            hasSubAttrs = true;
                            subAttributes.add(new AttributesLD(subAttrName,subAttrValue,subAttrValue,false,null));
                        } else if (!"value".equals(keyOne)){
                            JSONObject value2 = value.getJSONObject(keyOne);
                            subAttrName=keyOne;
                            subAttrType=value2.get("type").toString();
                            if ("Relationship".contentEquals(subAttrType)){
                                subAttrValue = value2.get("object").toString();
                            }else if ("Property".contentEquals(subAttrType)){
                                subAttrValue = value2.get("value").toString();
                            }else if ("GeoProperty".contentEquals(subAttrType)){
                                subAttrValue = value2.get("value").toString();
                            } else if ("RelationshipDetails".contains(keyOne)) {
                                value2.remove("id");
                                value2.remove("type");

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
        String subAttrType = value.get("type").toString();
        String subAttrValue = "";
        if ("Relationship".contentEquals(subAttrType)) {
            subAttrValue = value.get("object").toString();
        } else if ("Property".contentEquals(subAttrType)) {
            subAttrValue = value.get("value").toString();
        } else if ("GeoProperty".contentEquals(subAttrType)) {
            subAttrValue = value.get("value").toString();
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

    public String getDataFromRelationshipDetails(Entity entity, String data) {
        ArrayList<AttributesLD> entityAttributes = entity.getEntityAttrsLD();
        for(AttributesLD attr : entityAttributes) {
            if("servesDataset".contentEquals(attr.getAttrName())) {
                for(AttributesLD subAttr : attr.getSubAttrs()) {
                    if ("RelationshipDetails".equals(subAttr.getAttrName())) {
                        for (AttributesLD nestedSubAttr : subAttr.getSubAttrs()) {
                            if (data.equals(nestedSubAttr.getAttrName())) {
                                return nestedSubAttr.getAttrValue();
                            }
                        }
                    }

                }
            }
        }

        return null;
    }
}
