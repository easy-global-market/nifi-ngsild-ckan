package egm.io.nifi.processors.ckan.utils;

import egm.io.nifi.processors.ckan.model.DCATMetadata;
import egm.io.nifi.processors.ckan.ngsild.Entity;
import egm.io.nifi.processors.ckan.ngsild.NGSIUtils;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.Map;

import static egm.io.nifi.processors.ckan.ngsild.NGSIConstants.JSON_LD_FORMAT;

public class BuildDCATMetadata {
    public DCATMetadata getMetadataFromFlowFile(FlowFile flowFile, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];

        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        // Create the PreparedStatement to use for this FlowFile.
        Map<String, String> flowFileAttributes = flowFile.getAttributes();
        Map<String, String> newFlowFileAttributes = new CaseInsensitiveMap(flowFileAttributes);
        String[] keywords = newFlowFileAttributes.get("keyword").replace("[", "").replace("]", "").replaceAll("\"", "").split(",");

        return new DCATMetadata(
                newFlowFileAttributes.get("packageDescription"),
                newFlowFileAttributes.get("datasetTitle"),
                newFlowFileAttributes.get("contactPoint"),
                newFlowFileAttributes.get("contactName"),
                newFlowFileAttributes.get("contactEmail"),
                keywords,
                newFlowFileAttributes.get("publisherURL"),
                newFlowFileAttributes.get("spatialUri"),
                newFlowFileAttributes.get("spatialCoverage"),
                newFlowFileAttributes.get("temporalStart"),
                newFlowFileAttributes.get("temporalEnd"),
                newFlowFileAttributes.get("themes"),
                newFlowFileAttributes.get("version"),
                newFlowFileAttributes.get("landingPage"),
                newFlowFileAttributes.get("visibility"),
                newFlowFileAttributes.get("datasetRights"),
                null,
                null,
                null,
                JSON_LD_FORMAT,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }

    public void addMetadataFromEntity(Entity entity, DCATMetadata dcatMetadata) {
        dcatMetadata.setAccessURL(NGSIUtils.getSpecificAttributeValue(entity, "accessURL"));
        dcatMetadata.setAvailability(NGSIUtils.getSpecificAttributeValue(entity, "availability"));
        dcatMetadata.setMimeType(NGSIUtils.getSpecificAttributeValue(entity, "mediaType"));
        dcatMetadata.setLicense(NGSIUtils.getSpecificAttributeValue(entity, "license"));
        dcatMetadata.setDownloadURL(NGSIUtils.getSpecificAttributeValue(entity, "downloadURL"));
        dcatMetadata.setByteSize(NGSIUtils.getSpecificAttributeValue(entity, "byteSize"));
        dcatMetadata.setResourceRights(NGSIUtils.getSpecificAttributeValue(entity, "rights"));
        dcatMetadata.setResourceDescription(NGSIUtils.getSpecificAttributeValue(entity, "description"));
        dcatMetadata.setResourceName(NGSIUtils.getSpecificAttributeValue(entity, "title"));
        dcatMetadata.setLicenseType(NGSIUtils.getSpecificAttributeValue(entity, "licenseType"));
    }
}
