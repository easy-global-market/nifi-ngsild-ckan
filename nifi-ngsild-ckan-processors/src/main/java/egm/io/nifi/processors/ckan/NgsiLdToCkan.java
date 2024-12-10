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
import org.apache.nifi.logging.ComponentLog;
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
@Tags({"CKAN","ckan","sql", "put", "rdbms", "database", "create", "insert", "relational","NGSIv2", "NGSI","FIWARE"})
@CapabilityDescription("Create a CKAN resource, package and dataset if not exits using the information coming from and NGSI event converted to flow file." +
        "After insert all of the vales of the flow file content extraction the entities and attributes")
public class NgsiLdToCkan extends AbstractProcessor {
    protected static final PropertyDescriptor CKAN_HOST = new PropertyDescriptor.Builder()
            .name("CKAN Host")
            .displayName("CKAN Host")
            .description("FQDN/IP address where the CKAN server runs. Default value is localhost")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CKAN_PORT = new PropertyDescriptor.Builder()
            .name("CKAN Port")
            .displayName("CKAN Port")
            .description("Port where the CKAN server runs. Default value is 80")
            .required(true)
            .defaultValue("80")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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
            .description("The APi Key you are going o use in CKAN")
            .required(true)
            .defaultValue("XXXXXX")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor SSL = new PropertyDescriptor.Builder()
            .name("SSL")
            .displayName("SSL")
            .description("ssl for connection")
            .required(false)
            .defaultValue("false")
            .allowableValues("false", "true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ATTR_PERSISTENCE = new PropertyDescriptor.Builder()
            .name("attr-persistence")
            .displayName("Attribute Persistence")
            .description("The mode of storing the data inside of the table")
            .required(false)
            .allowableValues("row", "column")
            .defaultValue("row")
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
        properties.add(CKAN_HOST);
        properties.add(CKAN_PORT);
        properties.add(CKAN_VIEWER);
        properties.add(CKAN_API_KEY);
        properties.add(SSL);
        properties.add(ATTR_PERSISTENCE);
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
        final String[] host = {context.getProperty(CKAN_HOST).getValue()};
        final String port = context.getProperty(CKAN_PORT).getValue();
        final String apiKey = context.getProperty(CKAN_API_KEY).getValue();
        final String ckanViewer = context.getProperty(CKAN_VIEWER).getValue();
        final boolean ssl = context.getProperty(SSL).asBoolean();
        CKANBackend ckanBackend = new CKANBackend(apiKey, host, port, ssl, ckanViewer);
        ckanBackendAtomicReference.set(ckanBackend);
        getLogger().info("CKAN backend initialized with host: {}, port: {}, ssl: {}", host[0], port, ssl);

    }

    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile, ProcessSession session, CKANBackend ckanBackend) throws Exception {
        final boolean createDataStore = context.getProperty(CREATE_DATASTORE).asBoolean();
        final String attrPersistence = context.getProperty(ATTR_PERSISTENCE).getValue();
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
        aggregator = aggregator.getAggregator("row".equals(attrPersistence));
        try {

            final String orgName = ckanBackend.buildOrgName(organizationName, dcatMetadata);
            ArrayList<Entity> entities = event.getEntitiesLD();
            getLogger().info("[] Persisting data at NGSICKANSink (orgName=" + orgName+ ", ");
            getLogger().debug("DCAT metadata: {}" , dcatMetadata);

            for (Entity entity : entities) {
                final String pkgTitle = flowFile.getAttribute("datasetTitle");
                final String pkgName = ckanBackend.buildPkgName(pkgTitle, dcatMetadata);
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

                getLogger().info("[] Persisting data at NGSICKANSink (orgName=" + orgName
                        + ", pkgName=" + pkgName + ", resName=" + resName + ", data=(" + aggregation + ")");

                // Do try-catch only for metrics gathering purposes... after that, re-throw
                try {
                    if (aggregator instanceof CKANAggregator.RowAggregator) {
                        ckanBackend.persist(orgName, pkgName, pkgTitle, resName, aggregation, true, dcatMetadata,createDataStore);
                    } else {
                        ckanBackend.persist(orgName, pkgName, pkgTitle, resName, aggregation,  false, dcatMetadata,createDataStore);
                    } // if else

                } catch (Exception e) {
                    throw e;
                } // catch
            } // for

        }catch (Exception e){
            throw e;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        CKANBackend ckanBackend = ckanBackendAtomicReference.get();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        try {
            persistFlowFile(context, flowFile, session, ckanBackend);
            logger.info("inserted {} into CKAN", flowFile);
            session.getProvenanceReporter().send(flowFile, "report");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into CKAN due to {}", new Object[] {flowFile, e}, e);
            session.putAttribute(flowFile, "ckan.error.details", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

}
