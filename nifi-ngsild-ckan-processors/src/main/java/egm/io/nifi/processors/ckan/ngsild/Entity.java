package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<AttributesLD> entityAttrsLD;


    public Entity(String entityId, String entityType, ArrayList<AttributesLD> entityAttrsLD) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrsLD = entityAttrsLD;
    }

    public ArrayList<AttributesLD> getEntityAttrsLD() {
        return entityAttrsLD;
    }

    public void setEntityAttrsLD(ArrayList<AttributesLD> entityAttrsLD) {
        this.entityAttrsLD = entityAttrsLD;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    @Override
    public String toString() {
        return "Entity{" +
                "entityId='" + entityId + '\'' +
                ", entityType='" + entityType + '\'' +
                ", entityAttrsLD=" + entityAttrsLD +
                '}';
    }
}
