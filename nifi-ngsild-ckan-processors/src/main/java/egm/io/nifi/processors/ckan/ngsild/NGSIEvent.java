package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public ArrayList <Entity> entities;
    public ArrayList <Entity> entitiesLD;

    public NGSIEvent(long creationTime, ArrayList<Entity> entitiesLD ){
        this.creationTime = creationTime;
        this.entitiesLD = entitiesLD;
    }

    public ArrayList<Entity> getEntitiesLD() {
        return entitiesLD;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public ArrayList<Entity> getEntities() {
        return entities;
    }

    public void setEntities(ArrayList<Entity> entities) {
        this.entities = entities;
    }

    public long getRecvTimeTs() {
        return this.creationTime;
    } // getRecvTimeTs

}
