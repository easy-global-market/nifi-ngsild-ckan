package egm.io.nifi.processors.ckan;

import com.google.gson.JsonObject;
import egm.io.nifi.processors.ckan.model.DCATMetadata;
import egm.io.nifi.processors.ckan.ngsild.Entity;
import egm.io.nifi.processors.ckan.ngsild.NGSIEvent;
import egm.io.nifi.processors.ckan.ngsild.NGSIUtils;
import egm.io.nifi.processors.ckan.utils.BuildDCATMetadata;
import egm.io.nifi.processors.ckan.utils.CKANAggregator;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"CKAN", "ckan", "Open Data", "NGSI-LD", "NGSI", "FIWARE"})
@CapabilityDescription("Create a CKAN resource, package and dataset if not exists using the information coming from an NGSI-LD event converted to flow file." +
        "After insert all of the values of the flow file content extraction the entities and attributes")
public class NgsiLdToCkan extends AbstractProcessor {
    protected static final PropertyDescriptor CKAN_URL = new PropertyDescriptor.Builder()
            .name("CKAN URL")
            .displayName("CKAN URL")
            .description("URL where the CKAN server runs. Default value is http://localhost")
            .required(true)
            .defaultValue("http://localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_VIEWER = new PropertyDescriptor.Builder()
            .name("CKAN Viewer")
            .displayName("CKAN Viewer")
            .description("The CKAN resource page can contain one or more visualizations of the resource data or file contents (a table, a bar chart, a map, etc). These are commonly referred to as resource views.")
            .required(true)
            .defaultValue("datatables_view")
            .allowableValues("datatables_view", "text_view","image_view","video_view","audio_view","webpage_view")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_API_KEY = new PropertyDescriptor.Builder()
            .name("CKAN API Key")
            .displayName("CKAN API Key")
            .description("The API Key you are going to authenticate in CKAN")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_DATASTORE= new PropertyDescriptor.Builder()
            .name("create-datastore")
            .displayName("Create DataStore")
            .description("true or false, true applies create the DataStore resource")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    protected static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    private final AtomicReference<CKANBackend> ckanBackendAtomicReference = new AtomicReference<>();
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CKAN_URL);
        properties.add(CKAN_VIEWER);
        properties.add(CKAN_API_KEY);
        properties.add(CREATE_DATASTORE);
        properties.add(BATCH_SIZE);
        properties.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void setUpCKANBackend(final ProcessContext context) {
        final String url = context.getProperty(CKAN_URL).getValue();
        final String apiKey = context.getProperty(CKAN_API_KEY).getValue();
        final String ckanViewer = context.getProperty(CKAN_VIEWER).getValue();
        CKANBackend ckanBackend = new CKANBackend(url, apiKey, ckanViewer);
        ckanBackendAtomicReference.set(ckanBackend);
        getLogger().info("CKAN backend initialized with URL: {}", url);
    }

    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile, ProcessSession session, CKANBackend ckanBackend) throws Exception {
        final boolean createDataStore = context.getProperty(CREATE_DATASTORE).asBoolean();
        final NGSIUtils n = new NGSIUtils();
        final BuildDCATMetadata buildDCATMetadata = new BuildDCATMetadata();
        final DCATMetadata dcatMetadata= buildDCATMetadata.getMetadataFromFlowFile(flowFile,session);
        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session);
        final long creationTime = event.getCreationTime();
        final String organizationName = flowFile.getAttribute("X-CKAN-OrganizationName");
        CKANAggregator aggregator = new CKANAggregator() {
            @Override
            public void aggregate(Entity entity, long creationTime) {

            }
        };
        aggregator = aggregator.getAggregator();

        final String orgName = ckanBackend.buildOrgName(organizationName, dcatMetadata);
        ArrayList<Entity> entities = event.getEntitiesLD();
        getLogger().info("Persisting data at NGSICKANSink: orgName=" + orgName);
        getLogger().debug("DCAT metadata: {}" , dcatMetadata);

        for (Entity entity : entities) {

            // Update DCATMetadata with Distribution entity attributes
            buildDCATMetadata.addMetadataFromEntity(entity, dcatMetadata);

            final String pkgName = ckanBackend.buildPkgName(dcatMetadata);
            final String resName = ckanBackend.buildResName(entity, dcatMetadata);
            aggregator.initialize(entity);
            aggregator.aggregate(entity, creationTime);
            ArrayList<JsonObject> jsonObjects = CKANAggregator.linkedHashMapToJson(aggregator.getAggregationToPersist());
            String  aggregation= "";

            for (JsonObject jsonObject : jsonObjects) {
                if (aggregation.isEmpty()) {
                    aggregation = jsonObject.toString();
                } else {
                    aggregation += "," + jsonObject;
                }
            }

            getLogger().info("Persisting data at NGSICKANSink: orgName=" + orgName
                    + ", pkgName=" + pkgName + ", resName=" + resName + ", data=" + aggregation);

            ckanBackend.persist(orgName, pkgName, resName, aggregation, dcatMetadata, createDataStore);
        } // for

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        CKANBackend ckanBackend = ckanBackendAtomicReference.get();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            persistFlowFile(context, flowFile, session, ckanBackend);
            getLogger().info("inserted {} into CKAN", flowFile);
            session.getProvenanceReporter().send(flowFile, "report");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to insert {} into CKAN due to {}", new Object[] {flowFile, e}, e);
            session.putAttribute(flowFile, "ckan.error.details", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

}
