package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public ArrayList<Entity> entities;

    public NGSIEvent(long creationTime, ArrayList<Entity> entities) {
        this.creationTime = creationTime;
        this.entities = entities;
    }

    public ArrayList<Entity> getEntities() {
        return entities;
    }

    public long getCreationTime() {
        return creationTime;
    }
}
