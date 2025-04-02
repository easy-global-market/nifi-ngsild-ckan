package egm.io.nifi.processors.ckan.ngsild;

import java.util.ArrayList;

public class AttributesLD {
    public String attrName;
    public String attrType;
    public String attrValue;
    public String datasetId;
    public boolean hasSubAttrs;
    public ArrayList<AttributesLD> subAttrs;

    public boolean isHasSubAttrs() {
        return hasSubAttrs;
    }

    public ArrayList<AttributesLD> getSubAttrs() {
        return subAttrs;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public void setAttrType(String attrType) {
        this.attrType = attrType;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(String attrValue) {
        this.attrValue = attrValue;
    }
    public String getDatasetId() { return datasetId; }
    public void setDatasetId(String datasetId) { this.datasetId = datasetId; }

    public AttributesLD(String attrName, String attrType, String attrValue, String datasetId, boolean hasSubAttrs, ArrayList<AttributesLD> subAttrs) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.attrValue = attrValue;
        this.datasetId = datasetId;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs= subAttrs;

    }

    @Override
    public String toString() {
        return "AttributesLD{" +
                "attrName='" + attrName + '\'' +
                ", attrType='" + attrType + '\'' +
                ", attrValue='" + attrValue + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", hasSubAttrs=" + hasSubAttrs +
                ", subAttrs=" + (subAttrs != null ? subAttrs.toString() : "null") +
                '}';
    }
}
