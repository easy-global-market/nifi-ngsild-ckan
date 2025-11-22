package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public ArrayList<Entity> entitiesLD;

    public NGSIEvent(long creationTime, ArrayList<Entity> entitiesLD) {
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

    public long getRecvTimeTs() {
        return this.creationTime;
    } // getRecvTimeTs

}
