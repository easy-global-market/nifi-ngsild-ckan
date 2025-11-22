package egm.io.nifi.processors.ckan.utils;

import egm.io.nifi.processors.ckan.model.DCATMetadata;
import egm.io.nifi.processors.ckan.ngsild.Entity;
import egm.io.nifi.processors.ckan.ngsild.NGSIUtils;

import static egm.io.nifi.processors.ckan.ngsild.NGSIConstants.JSON_LD_FORMAT;

public class BuildDCATMetadata {

    public static DCATMetadata getMetadataFromEntity(Entity entity) {
        DCATMetadata dcatMetadata = new DCATMetadata();
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
        dcatMetadata.setFormat(JSON_LD_FORMAT);

        dcatMetadata.setPackageDescription(NGSIUtils.getSpecificDatasetValue(entity, "description"));
        dcatMetadata.setPackageName(NGSIUtils.getSpecificDatasetValue(entity, "title"));
        dcatMetadata.setContactPoint(NGSIUtils.getSpecificDatasetValue(entity, "contactPoint"));
        dcatMetadata.setContactName(NGSIUtils.getSpecificDatasetValue(entity, "contactName"));
        dcatMetadata.setContactEmail(NGSIUtils.getSpecificDatasetValue(entity, "contactEmail"));
        String rawKeywords = NGSIUtils.getSpecificDatasetValue(entity, "keywords");
        if (rawKeywords != null) {
            String[] keywords = rawKeywords.replace("[", "")
                    .replace("]", "")
                    .replaceAll("\"", "").split(",");
            dcatMetadata.setKeywords(keywords);
        }
        dcatMetadata.setPublisherURL(NGSIUtils.getSpecificDatasetValue(entity, "publisherURL"));
        dcatMetadata.setSpatialUri(NGSIUtils.getSpecificDatasetValue(entity, "spatialUri"));
        dcatMetadata.setSpatialCoverage(NGSIUtils.getSpecificDatasetValue(entity, "spatial"));
        // TODO parse start and end dates for temporal
        dcatMetadata.setTemporalStart(NGSIUtils.getSpecificDatasetValue(entity, "temporal"));
        dcatMetadata.setTemporalEnd(NGSIUtils.getSpecificDatasetValue(entity, "temporal"));
        dcatMetadata.setThemes(NGSIUtils.getSpecificDatasetValue(entity, "theme"));
        dcatMetadata.setVersion(NGSIUtils.getSpecificDatasetValue(entity, "version"));
        dcatMetadata.setLandingPage(NGSIUtils.getSpecificDatasetValue(entity, "landingPage"));
        dcatMetadata.setVisibility(NGSIUtils.getSpecificDatasetValue(entity, "visibility"));
        dcatMetadata.setDatasetRights(NGSIUtils.getSpecificDatasetValue(entity, "accessRights"));

        return dcatMetadata;
    }
}
