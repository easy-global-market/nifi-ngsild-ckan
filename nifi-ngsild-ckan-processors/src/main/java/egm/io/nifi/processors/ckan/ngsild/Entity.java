package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<Attributes> entityAttrs;


    public Entity(String entityId, String entityType, ArrayList<Attributes> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrs = entityAttrs;
    }

    public ArrayList<Attributes> getEntityAttrs() {
        return entityAttrs;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "entityId='" + entityId + '\'' +
                ", entityType='" + entityType + '\'' +
                ", entityAttrsLD=" + entityAttrs +
                '}';
    }
}
