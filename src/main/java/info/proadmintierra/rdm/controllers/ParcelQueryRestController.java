package info.proadmintierra.rdm.controllers;

import java.awt.Image;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.ResultSet;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import info.proadmintierra.rdm.queries.BasicQuery;
import info.proadmintierra.rdm.queries.EconomicQuery;
import info.proadmintierra.rdm.queries.IGACPropertyRecordCardQuery;
import info.proadmintierra.rdm.queries.LegalQuery;
import info.proadmintierra.rdm.queries.PartyQuery;
import info.proadmintierra.rdm.queries.PhysicalQuery;
import info.proadmintierra.rdm.drivers.Postgres;

/**
 * ParcelQueryRestController
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = { "http://localhost:4200", "*" })
public class ParcelQueryRestController {

    @Value("${rdm.schema}")
    private String rdmSchema;
    @Value("${spring.datasource.url}")
    private String connectionString;
    @Value("${spring.datasource.username}")
    private String connectionUser;
    @Value("${spring.datasource.password}")
    private String connectionPassword;
    @Value("${spring.datasource.driver-class-name}")
    private String classForName;
    @Value("${geoserver.url}")
    private String geoserverUrl;

    @GetMapping(value = "/public/parcel", produces = { "application/json" })
    public String getParcelBasicInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = BasicQuery.getQuery(this.rdmSchema, null, fmi, cadastralCode, null, false, false);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/public/parcel/geometry", produces = { "application/json" })
    public String getParcelBasicInfo(@RequestParam(required = false) Integer id) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = "select st_asgeojson(ST_Transform(t.poligono_creado, 3857)) from " + rdmSchema + ".terreno t join "
                    + rdmSchema + ".uebaunit u on t.t_id=u.ue_terreno join " + rdmSchema
                    + ".predio p on p.t_id=u.baunit_predio where p.t_id = " + id;
            // sql = "select st_asgeojson(ST_Transform(terreno.poligono_creado, 4326)) from
            // " + rdmSchema + ".terreno where terreno.t_id = " + id;
            System.out.println("SQL: " + sql);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/public/parcel/geometry/png", produces = { "image/png" })
    public @ResponseBody byte[] getImageParcelBasicInfo(@RequestParam(required = true) Integer id) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = "select st_xmin(bbox.g) as xmin, st_ymin(bbox.g) as ymin,st_xmax(bbox.g) as xmax, st_ymax(bbox.g) as ymax from (select st_envelope(poligono_creado) as g from "
                    + rdmSchema + ".vw_terreno where t_id=" + id + ") bbox";
            ResultSet response = conn.query(sql);

            double xmin = response.getDouble(1);
            double ymin = response.getDouble(2);
            double xmax = response.getDouble(3);
            double ymax = response.getDouble(4);

            conn.disconnect();

            URIBuilder url = new URIBuilder(this.geoserverUrl);
            url.addParameter("SERVICE", "WMS");
            url.addParameter("VERSION", "1.1.1");
            url.addParameter("REQUEST", "GetMap");
            url.addParameter("FORMAT", "image/png");
            url.addParameter("TRANSPARENT", "true");
            // url.addParameter("STYLES", "");
            url.addParameter("LAYERS", "LADM:sat_basic_query");
            url.addParameter("CQL_FILTER",
                    "(t_id=" + id + " AND layer='parcel') OR (layer='context' AND t_id<>" + id + ")");
            url.addParameter("SRS", "EPSG:3857");
            url.addParameter("WIDTH", "769");
            url.addParameter("HEIGHT", "763");
            url.addParameter("BBOX", xmin + "," + ymin + "," + xmax + "," + ymax);
            
            System.out.println(url.toString());
            URL ourl = new URL(url.toString());
            URLConnection geos_conn = ourl.openConnection();
            InputStream in = geos_conn.getInputStream();
            return IOUtils.toByteArray(in);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping(value = "/public/terrain/geometry", produces = { "application/json" })
    public String getTerrainBasicInfo(@RequestParam(required = false) Integer id) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = "select st_asgeojson(ST_Transform(terreno.poligono_creado, 3857)) from " + rdmSchema
                    + ".terreno where terreno.t_id = " + id;
            // sql = "select st_asgeojson(ST_Transform(terreno.poligono_creado, 4326)) from
            // " + rdmSchema + ".terreno where terreno.t_id = " + id;
            System.out.println("SQL: " + sql);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/private/parcel/economic", produces = { "application/json" })
    public String getParcelEconomicInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = EconomicQuery.getQuery(this.rdmSchema, null, fmi, cadastralCode, null, true, true);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/private/parcel/legal", produces = { "application/json" })
    public String getParcelLegalInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = LegalQuery.getQuery(this.rdmSchema, null, fmi, cadastralCode, null);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/private/parcel/physical", produces = { "application/json" })
    public String getParcelPhysicalInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = PhysicalQuery.getQuery(this.rdmSchema, null, fmi, cadastralCode, null, true);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/private/parcel/igac_property_record_card", produces = { "application/json" })
    public String getParcelIGACPropertyRecordCardInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = IGACPropertyRecordCardQuery.getQuery(this.rdmSchema, null, fmi, cadastralCode, null, true);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    @GetMapping(value = "/private/parcel/party", produces = { "application/json" })
    public String getParcelPartyInfo(@RequestParam(required = false) String nupre,
            @RequestParam(required = false) String cadastralCode, @RequestParam(required = false) String fmi) {
        String sql = "";
        try {
            Postgres conn = new Postgres();
            conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
            sql = PartyQuery.getQuery(this.rdmSchema, fmi, cadastralCode, nupre);
            String response = conn.queryToString(sql);
            conn.disconnect();
            if (response != null)
                return response;
            else
                return "{\"error\":\"No se encontraron registros.\"}";
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }
}