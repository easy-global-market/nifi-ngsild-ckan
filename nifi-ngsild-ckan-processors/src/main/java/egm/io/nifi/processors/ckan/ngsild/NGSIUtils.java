package egm.io.nifi.processors.ckan.ngsild;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ckan.ngsild.NGSIConstants.*;


public class NGSIUtils {

    private static final Logger logger = LoggerFactory.getLogger(NGSIUtils.class);
    public static List<String> IGNORED_KEYS_ON_ATTRIBUTES =
            List.of(NGSILD_TYPE, NGSILD_VALUE, NGSILD_OBJECT, NGSILD_JSON, NGSILD_CREATED_AT, NGSILD_MODIFIED_AT, NGSILD_DATASET_ID);

    public static String getSpecificAttributeValue(Entity entity, String attributeName) {
        ArrayList<Attributes> entityAttributes = entity.getEntityAttrs();
        for (Attributes attr : entityAttributes) {
            if (attr.getAttrName().equalsIgnoreCase(attributeName))
                return attr.getAttrValue();
        }
        logger.info("Did not find attribute {} in entity {}", attributeName, entity.getEntityId());

        return null;
    }

    public static String getSpecificDatasetValue(Entity entity, String attributeName) {
        Attributes servesDatasetAttribute =
                entity.getEntityAttrs().stream()
                        .filter(attr -> DCAT_SERVES_DATASET.equals(attr.getAttrName()))
                        .findFirst().orElse(null);
        if (servesDatasetAttribute == null) {
            logger.warn("Did not find attribute {} in entity {}", DCAT_SERVES_DATASET, entity.getEntityId());
            return null;
        }
        for (Attributes attr : servesDatasetAttribute.getSubAttrs()) {
            if (attr.getAttrName().equalsIgnoreCase(attributeName))
                return attr.getAttrValue();
        }
        logger.info("Did not find dataset attribute {} in entity {}", attributeName, entity.getEntityId());

        return null;
    }

    public NGSIEvent getEventFromFlowFile(FlowFile flowFile, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];

        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);
        long creationTime = flowFile.getEntryDate();
        JSONObject content = new JSONObject(flowFileContent);
        JSONArray data;
        String entityType;
        String entityId;
        ArrayList<Entity> entities = new ArrayList<>();
        NGSIEvent event;

        data = (JSONArray) content.get(NGSILD_DATA);
        for (int i = 0; i < data.length(); i++) {
            JSONObject lData = data.getJSONObject(i);
            entityId = lData.getString(NGSILD_ID);
            entityType = parseEntityTypes(lData);
            ArrayList<Attributes> attributes = new ArrayList<>();
            Iterator<String> keys = lData.keys();

            while (keys.hasNext()) {
                String key = keys.next();
                if (!NGSILD_ID.equals(key) && !NGSILD_TYPE.equals(key) && !NGSILD_CONTEXT.equals(key)) {
                    Object object = lData.get(key);
                    if (object instanceof JSONArray) {
                        // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                        JSONArray values = lData.getJSONArray(key);
                        for (int j = 0; j < values.length(); j++) {
                            JSONObject value = values.getJSONObject(j);
                            Attributes attribute = parseNgsiLdAttribute(key, value);
                            addAttributeIfValid(attributes, attribute);
                        }
                    } else if (object instanceof JSONObject) {
                        JSONObject value = lData.getJSONObject(key);
                        Attributes attribute = parseNgsiLdAttribute(key, value);
                        addAttributeIfValid(attributes, attribute);

                    }
                }
            }
            entities.add(new Entity(entityId, entityType, attributes));
        }
        event = new NGSIEvent(creationTime, entities);
        return event;
    }

    private Attributes parseNgsiLdAttribute(String key, JSONObject value) {
        String attrType;
        String attrValue = "";
        String datasetId;
        ArrayList<Attributes> subAttributes = new ArrayList<>();

        attrType = value.getString(NGSILD_TYPE);
        datasetId = value.optString(NGSILD_DATASET_ID);
        if (NGSILD_RELATIONSHIP.contentEquals(attrType)) {
            attrValue = value.get(NGSILD_OBJECT).toString();
        } else if (NGSILD_PROPERTY.contentEquals(attrType)) {
            attrValue = value.get(NGSILD_VALUE).toString();
        } else if (NGSILD_GEOPROPERTY.contentEquals(attrType)) {
            attrValue = value.get(NGSILD_VALUE).toString();
        }
        Iterator<String> keysOneLevel = value.keys();
        while (keysOneLevel.hasNext()) {
            String keyOne = keysOneLevel.next();
            if (IGNORED_KEYS_ON_ATTRIBUTES.contains(keyOne)) {
                // Do Nothing
            } else if (keyOne.equals(NGSILD_OBSERVED_AT) || keyOne.equals(NGSILD_UNIT_CODE)) {
                subAttributes.add(
                        new Attributes(keyOne, "NonReifiedProperty", value.getString(keyOne), "", false, null)
                );
            } else {
                JSONObject value2 = value.getJSONObject(keyOne);
                String subAttrType = value2.get(NGSILD_TYPE).toString();
                if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)) {
                    String subAttrValue = value2.get(NGSILD_OBJECT).toString();
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if (NGSILD_PROPERTY.contentEquals(subAttrType)) {
                    String subAttrValue = value2.get(NGSILD_VALUE).toString();
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if (NGSILD_GEOPROPERTY.contentEquals(subAttrType)) {
                    String subAttrValue = value2.get(NGSILD_VALUE).toString();
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if ("entity".equals(keyOne)) {
                    value2.remove(NGSILD_ID);
                    value2.remove(NGSILD_TYPE);

                    for (String relationKey : value2.keySet()) {
                        Object object = value2.get(relationKey);
                        if (object instanceof JSONArray) {
                            // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                            JSONArray valuesArray = value2.getJSONArray(relationKey);
                            for (int j = 0; j < valuesArray.length(); j++) {
                                JSONObject valueObject = valuesArray.getJSONObject(j);
                                Attributes subAttribute = parseNgsiLdSubAttribute(relationKey, valueObject);
                                addAttributeIfValid(subAttributes, subAttribute);
                            }
                        } else if (object instanceof JSONObject) {
                            Attributes subAttribute = parseNgsiLdSubAttribute(relationKey, (JSONObject) object);
                            addAttributeIfValid(subAttributes, subAttribute);
                        } else {
                            logger.info("Sub Attribute {} has unexpected value type: {}", relationKey, object.getClass());
                        }
                    }
                }
            }
        }
        return new Attributes(key, attrType, attrValue, datasetId, !subAttributes.isEmpty(), subAttributes);
    }

    private String parseEntityTypes(JSONObject temporalEntity) {
        if (temporalEntity.get("type") instanceof JSONArray) {
            return temporalEntity.getJSONArray("type")
                    .toList()
                    .stream().map(type -> (String) type)
                    .sorted()
                    .collect(Collectors.joining("_"));
        } else {
            return temporalEntity.getString("type");
        }
    }

    private Attributes parseNgsiLdSubAttribute(String key, JSONObject value) {
        String subAttrType = value.get(NGSILD_TYPE).toString();
        String subAttrValue = "";
        if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_OBJECT).toString();
        } else if (NGSILD_PROPERTY.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_VALUE).toString();
        } else if (NGSILD_GEOPROPERTY.contentEquals(subAttrType)) {
            subAttrValue = value.get(NGSILD_VALUE).toString();
        }

        return new Attributes(key.toLowerCase(), subAttrType, subAttrValue, "", false, null);
    }

    // When this processor is used in a flow with a `Join Enrichment` processor, it harmonizes JSON among all processed entities,
    // for instance adding attributes which are not present by default in an entity.
    // In this case, these attributes are null or can have a null value.
    // So we filter out attributes that contain a null value or whose whole value is null
    private void addAttributeIfValid(List<Attributes> attributes, Attributes attribute) {
        if (attribute != null &&
                attribute.getAttrValue() != null &&
                !Objects.equals(attribute.getAttrValue(), "null"))
            attributes.add(attribute);
    }
}
