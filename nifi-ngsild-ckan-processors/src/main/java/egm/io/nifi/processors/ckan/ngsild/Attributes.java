package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class Attributes {
    public String attrName;
    public String attrType;
    public String attrValue;
    public String datasetId;
    public boolean hasSubAttrs;
    public ArrayList<Attributes> subAttrs;

    public Attributes(String attrName, String attrType, String attrValue, String datasetId, boolean hasSubAttrs, ArrayList<Attributes> subAttrs) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.attrValue = attrValue;
        this.datasetId = datasetId;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs = subAttrs;

    }

    public boolean isHasSubAttrs() {
        return hasSubAttrs;
    }

    public ArrayList<Attributes> getSubAttrs() {
        return subAttrs;
    }

    public String getAttrName() {
        return attrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public String getDatasetId() {
        return datasetId;
    }

    @Override
    public String toString() {
        return "Attributes{" +
                "attrName='" + attrName + '\'' +
                ", attrType='" + attrType + '\'' +
                ", attrValue='" + attrValue + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", hasSubAttrs=" + hasSubAttrs +
                ", subAttrs=" + (subAttrs != null ? subAttrs.toString() : "null") +
                '}';
    }
}
