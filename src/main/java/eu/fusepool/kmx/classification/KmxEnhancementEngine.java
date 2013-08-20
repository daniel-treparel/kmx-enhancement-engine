package eu.fusepool.kmx.classification;


import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Map;
import java.util.logging.Level;
import org.apache.clerezza.rdf.core.LiteralFactory;

//import org.apache.clerezza.rdf.core.LiteralFactory;
import org.apache.clerezza.rdf.core.MGraph;
import org.apache.clerezza.rdf.core.UriRef;
import org.apache.clerezza.rdf.core.impl.PlainLiteralImpl;
import org.apache.clerezza.rdf.core.impl.TripleImpl;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.felix.scr.annotations.Reference;
import org.apache.stanbol.enhancer.servicesapi.ContentItem;
import org.apache.stanbol.enhancer.servicesapi.EngineException;
import org.apache.stanbol.enhancer.servicesapi.EnhancementEngine;
import org.apache.stanbol.enhancer.servicesapi.ServiceProperties;
import org.apache.stanbol.enhancer.servicesapi.impl.AbstractEnhancementEngine;
import org.osgi.framework.Constants;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.clerezza.rdf.core.serializedform.Parser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.BinaryCodec;
import org.apache.commons.io.IOUtils;
import org.apache.stanbol.enhancer.servicesapi.helper.EnhancementEngineHelper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import static org.apache.stanbol.enhancer.servicesapi.rdf.Properties.ENHANCER_CONFIDENCE;
import static org.apache.stanbol.enhancer.servicesapi.rdf.Properties.ENHANCER_ENTITY_LABEL; 

@Component(immediate = true, metatype = true)
@Service
@Properties(value={
     @Property(name=EnhancementEngine.PROPERTY_NAME, value=KmxEnhancementEngine.DEFAULT_ENGINE_NAME),
     @Property(name=Constants.SERVICE_RANKING,intValue=KmxEnhancementEngine.DEFAULT_SERVICE_RANKING)
})
public class KmxEnhancementEngine 
        extends AbstractEnhancementEngine<IOException,RuntimeException> 
        implements EnhancementEngine, ServiceProperties {

    /// this is how by which the engine is identified in a chain
    public static final String DEFAULT_ENGINE_NAME = "KMXEngine";
    public static final String DEFAULT_WEBSERVICE_URL = "http://192.168.1.87:9090/kmx/api/v1/"; 
    @Property(value = DEFAULT_WEBSERVICE_URL)
    public static final String WEBSERVICE_URL = "eu.fusepool.kmx.classification.kmxEnhancementEngine.serverURL";
    @Property()
    public static final String WEBSERVICE_USERNAME = "eu.fusepool.kmx.classification.kmxEnhancementEngine.serverUsername";
    @Property()
    public static final String WEBSERVICE_PASSWORD = "eu.fusepool.kmx.classification.kmxEnhancementEngine.serverPassword";
    @Property()
    public static final String WEBSERVICE_SERVER_KEY = "eu.fusepool.kmx.classification.kmxEnhancementEngine.serverKey";
    
    /**
     * Default value for the {@link Constants#SERVICE_RANKING} used by this engine.
     * This is a negative value to allow easy replacement by this engine depending
     * to a remote service with one that does not have this requirement
     */
    public static final int DEFAULT_SERVICE_RANKING = 100;
    /**
     * The default value for the Execution of this Engine. Currently set to
     * {@link ServiceProperties#ORDERING_EXTRACTION_ENHANCEMENT}
     */
    public static final Integer defaultOrder = ORDERING_EXTRACTION_ENHANCEMENT;
	
    private static final Logger log = LoggerFactory.getLogger(KmxEnhancementEngine.class);
    
    private String sessionKey;
    private String username;
    private String password;
    private String kmxURL;
    //@Reference
    //private Parser parser;

    @Override
    @SuppressWarnings("unchecked")
    protected void activate(ComponentContext ce) throws IOException, ConfigurationException {
        super.activate(ce);
        Dictionary<String, Object> properties = ce.getProperties();
        kmxURL = (String) properties.get(WEBSERVICE_URL);
        username = (String) properties.get(WEBSERVICE_USERNAME);
        password = (String) properties.get(WEBSERVICE_PASSWORD);
        sessionKey = (String) properties.get(WEBSERVICE_SERVER_KEY);
        log.info("activating ...");
	System.out.println("activate " + kmxURL);
	System.out.println("activate " + username);
	System.out.println("activate " + password);
	System.out.println("activate " + sessionKey);
    }

    @Override
    protected void deactivate(ComponentContext ce) {
        super.deactivate(ce);
    }

    @Override
    public int canEnhance(ContentItem ci) throws EngineException {
        return ENHANCE_SYNCHRONOUS;
    }

    private double requestKMXClassification(String text) throws EngineException, IOException {
        // hardcoded model for now: 7557
        String svm_model_id = "7621"; //   illiad / Romeo&Juliet
        
        URL requestUrl;
        String requestString = kmxURL + "model/svm/"+ svm_model_id + 
                "/apply?session=" + sessionKey;
        try {
            requestUrl = new URL(requestString.toString());
            System.out.println(" > kmx request: " + requestUrl);
        } catch (MalformedURLException e) {
            throw new EngineException("Unable to build valid request URL for " + requestString);
        }
        
        String authString = username + ":" + password;
        byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
        String authStringEnc = new String(authEncBytes);
        
        // TODO: how do we catch and report errors?
        HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();
        connection.setUseCaches (false);
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", "Basic " + authStringEnc);
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        String body;
        try {
            JSONObject data = new JSONObject();
            data.put("text", text);
            body = data.toString();
        } catch (JSONException ex) {
            throw new IOException("Unable to serialize text", ex);
        }
        connection.setRequestProperty("Content-Length", ""
                + Integer.toString(body.getBytes().length));
        DataOutputStream wr = new DataOutputStream(
                connection.getOutputStream());
        wr.writeBytes(body);
        wr.flush();
        wr.close();
        
        String result = IOUtils.toString(connection.getInputStream());
        //System.out.println(result);
        
        try {
            JSONObject root = new JSONObject(result);
            System.out.println(root.toString());
            if (root.has("error_message")) {
                String msg = root.getString("error_message");
                if (root.has("detail_message")) {
                    msg = root.getString("detail_message");
                }
                throw new EngineException("KMX responded with:\n" + msg);
            }
            //System.out.println(root.);
            if (root.has("Positive")) {
                return Double.parseDouble(root.getString("Positive"));
            }
        } catch (JSONException e) {
            log.error("Unable to parse Response for Request " + requestUrl);
            log.error("ResponseData: \n" + result);
            throw new IOException("Unable to parse JSON from Results for Request " + requestUrl, e);
        }
        
        return 0.5; // unreachable
    }
    
    @Override
    public void computeEnhancements(ContentItem ci) throws EngineException {
        System.out.println("computing ...");
        
        try {
            Reader in = new InputStreamReader(ci.getStream(), "utf-8");
            StringWriter stringWriter = new StringWriter();
            // TODO: user IOUtils.tostring?
            for (int ch = in.read(); ch != -1; ch = in.read()) {
                stringWriter.write(ch);
            }
            String contentAsString = stringWriter.toString();   
            double score = requestKMXClassification(contentAsString);




            UriRef contentItemId = ci.getUri();
            MGraph graph = ci.getMetadata();
            LiteralFactory literalFactory = LiteralFactory.getInstance();

            UriRef entityAnnotation = EnhancementEngineHelper.createEntityEnhancement(
                    graph, this, contentItemId); 

            graph.add(new TripleImpl(entityAnnotation,
                    ENHANCER_ENTITY_LABEL,
                    new PlainLiteralImpl("KMX_classifier_1"))); 
            graph.add(new TripleImpl(entityAnnotation,
                    ENHANCER_CONFIDENCE, 
                    literalFactory.createTypedLiteral(score)));
        } catch (IOException ex) {
            throw new EngineException(ex);
        }
    }

    @Override
    public Map<String, Object> getServiceProperties() {
        return Collections.unmodifiableMap(Collections.singletonMap(
                ENHANCEMENT_ENGINE_ORDERING, (Object) defaultOrder));
    }

}
