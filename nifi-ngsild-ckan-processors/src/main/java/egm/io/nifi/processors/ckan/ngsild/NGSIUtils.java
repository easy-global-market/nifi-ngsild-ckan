package egm.io.nifi.processors.ckan.ngsild;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        JsonObject content = JsonParser.parseString(flowFileContent).getAsJsonObject();
        JsonArray data;
        String entityType;
        String entityId;
        ArrayList<Entity> entities = new ArrayList<>();
        NGSIEvent event;

        data = content.getAsJsonArray(NGSILD_DATA);
        for (int i = 0; i < data.size(); i++) {
            JsonObject lData = data.get(i).getAsJsonObject();
            entityId = lData.get(NGSILD_ID).getAsString();
            entityType = parseEntityTypes(lData);
            ArrayList<Attributes> attributes = new ArrayList<>();

            for (String key : lData.keySet()) {
                if (!NGSILD_ID.equals(key) && !NGSILD_TYPE.equals(key) && !NGSILD_CONTEXT.equals(key)) {
                    JsonElement element = lData.get(key);
                    if (element.isJsonArray()) {
                        // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                        JsonArray values = element.getAsJsonArray();
                        for (int j = 0; j < values.size(); j++) {
                            JsonObject value = values.get(j).getAsJsonObject();
                            Attributes attribute = parseNgsiLdAttribute(key, value);
                            addAttributeIfValid(attributes, attribute);
                        }
                    } else if (element.isJsonObject()) {
                        JsonObject value = element.getAsJsonObject();
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

    private Attributes parseNgsiLdAttribute(String key, JsonObject value) {
        String attrType;
        String attrValue = "";
        String datasetId;
        ArrayList<Attributes> subAttributes = new ArrayList<>();

        attrType = value.get(NGSILD_TYPE).getAsString();
        datasetId = value.has(NGSILD_DATASET_ID) ? value.get(NGSILD_DATASET_ID).getAsString() : "";
        
        if (NGSILD_RELATIONSHIP.contentEquals(attrType)) {
            attrValue = attributeValueToString(value.get(NGSILD_OBJECT));
        } else if (NGSILD_PROPERTY.contentEquals(attrType)) {
            attrValue = attributeValueToString(value.get(NGSILD_VALUE));
        } else if (NGSILD_GEOPROPERTY.contentEquals(attrType)) {
            attrValue = attributeValueToString(value.get(NGSILD_VALUE));
        }

        for (String keyOne : value.keySet()) {
            if (IGNORED_KEYS_ON_ATTRIBUTES.contains(keyOne)) {
                // Do Nothing
            } else if (keyOne.equals(NGSILD_OBSERVED_AT) || keyOne.equals(NGSILD_UNIT_CODE)) {
                subAttributes.add(
                    new Attributes(keyOne, "NonReifiedProperty", value.get(keyOne).getAsString(), "", false, null)
                );
            } else {
                JsonObject value2 = value.getAsJsonObject(keyOne);
                String subAttrType = value2.get(NGSILD_TYPE).getAsString();
                if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)) {
                    String subAttrValue = attributeValueToString(value2.get(NGSILD_OBJECT));
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if (NGSILD_PROPERTY.contentEquals(subAttrType)) {
                    String subAttrValue = attributeValueToString(value2.get(NGSILD_VALUE));
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if (NGSILD_GEOPROPERTY.contentEquals(subAttrType)) {
                    String subAttrValue = attributeValueToString(value2.get(NGSILD_VALUE));
                    subAttributes.add(new Attributes(keyOne, subAttrType, subAttrValue, "", false, null));
                } else if ("entity".equals(keyOne)) {
                    value2.remove(NGSILD_ID);
                    value2.remove(NGSILD_TYPE);

                    for (String relationKey : value2.keySet()) {
                        JsonElement object = value2.get(relationKey);
                        if (object.isJsonArray()) {
                            // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                            JsonArray valuesArray = object.getAsJsonArray();
                            for (int j = 0; j < valuesArray.size(); j++) {
                                JsonObject valueObject = valuesArray.get(j).getAsJsonObject();
                                Attributes subAttribute = parseNgsiLdSubAttribute(relationKey, valueObject);
                                addAttributeIfValid(subAttributes, subAttribute);
                            }
                        } else if (object.isJsonObject()) {
                            Attributes subAttribute = parseNgsiLdSubAttribute(relationKey, object.getAsJsonObject());
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

    private String parseEntityTypes(JsonObject temporalEntity) {
        JsonElement typeElement = temporalEntity.get("type");
        if (typeElement.isJsonArray()) {
            JsonArray typeArray = typeElement.getAsJsonArray();
            List<String> types = new ArrayList<>();
            for (int i = 0; i < typeArray.size(); i++) {
                types.add(typeArray.get(i).getAsString());
            }
            return types.stream().sorted().collect(Collectors.joining("_"));
        } else {
            return typeElement.getAsString();
        }
    }

    private Attributes parseNgsiLdSubAttribute(String key, JsonObject value) {
        String subAttrType = value.get(NGSILD_TYPE).getAsString();
        String subAttrValue = "";
        if (NGSILD_RELATIONSHIP.contentEquals(subAttrType)) {
            subAttrValue = attributeValueToString(value.get(NGSILD_OBJECT));
        } else if (NGSILD_PROPERTY.contentEquals(subAttrType)) {
            subAttrValue = attributeValueToString(value.get(NGSILD_VALUE));
        } else if (NGSILD_GEOPROPERTY.contentEquals(subAttrType)) {
            subAttrValue = attributeValueToString(value.get(NGSILD_VALUE));
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

    private String attributeValueToString(JsonElement element) {
        if (element.isJsonPrimitive()) {
            return element.getAsString();
        } else {
            return element.toString();
        }
    }
}
