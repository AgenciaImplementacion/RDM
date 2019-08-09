package info.proadmintierra.rdm.controllers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.ResultSet;
import java.text.DecimalFormat;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.utils.URIBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import info.proadmintierra.rdm.drivers.Postgres;
import info.proadmintierra.rdm.queries.BasicQuery;
import info.proadmintierra.rdm.queries.EconomicQuery;
import info.proadmintierra.rdm.queries.IGACPropertyRecordCardQuery;
import info.proadmintierra.rdm.queries.LegalQuery;
import info.proadmintierra.rdm.queries.PartyQuery;
import info.proadmintierra.rdm.queries.PhysicalQuery;

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
	@Value("${databaseVU.datasource}")
	private String databaseVUDatasource;
	@Value("${databaseVU.datasource.username}")
	private String databaseVUUsername;
	@Value("${databaseVU.datasource.password}")
	private String databaseVUPassword;

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
			int padding = 20;
			Postgres conn = new Postgres();
			conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
			sql = "select st_xmin(bbox.g) as xmin, st_ymin(bbox.g) as ymin,st_xmax(bbox.g) as xmax, st_ymax(bbox.g) as ymax from (select st_envelope(poligono_creado) as g from "
					+ rdmSchema + ".vw_terreno where t_id=" + id + ") bbox";
			ResultSet response = conn.query(sql);

			double xmin = response.getDouble(1) - padding;
			double ymin = response.getDouble(2) - padding;
			double xmax = response.getDouble(3) + padding;
			double ymax = response.getDouble(4) + padding;

			double x = xmax - xmin;
			double y = ymax - ymin;

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
					"(id=" + id + " AND layer='parcel') OR (layer='context' AND id<>" + id + ")");
			url.addParameter("SRS", "EPSG:3857");
			if (x > y) {
				ymin -= (x - y) / 2;
				ymax += (x - y) / 2;
			} else if (y > x) {
				xmin -= (y - x) / 2;
				xmax += (y - x) / 2;
			}
			url.addParameter("WIDTH", "700");
			url.addParameter("HEIGHT", "700");
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

	@GetMapping(value = "/private/parcel/affectations", produces = { "application/json" })
	public String getAffectationsInfo(@RequestParam(required = false) Integer id) {
		String sql = "";
		try {

			Postgres connRDM = new Postgres();
			connRDM.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);

			Postgres conn = new Postgres();
			conn.connect(this.databaseVUDatasource, this.databaseVUUsername, this.databaseVUPassword,
					this.classForName);

			String sqlObjects = "SELECT * FROM vu.objects_special_regime;";
			ResultSet resultsetObjects = conn.getResultSetFromSql(sqlObjects);

			JsonArray restrictions = new JsonArray();

			while (resultsetObjects.next()) {

				String sqlCategories = "SELECT * FROM vu.category WHERE id_object_special_regime = "
						+ resultsetObjects.getString("id") + ";";

				ResultSet resultsetCategories = conn.getResultSetFromSql(sqlCategories);

				URL url = new URL(resultsetObjects.getString("wsurl"));
				HttpURLConnection connectionApiUrl = (HttpURLConnection) url.openConnection();
				connectionApiUrl.setRequestMethod("GET");
				int responseCode = connectionApiUrl.getResponseCode();
				if (responseCode == 200) {

					BufferedReader in = new BufferedReader(new InputStreamReader(connectionApiUrl.getInputStream()));
					String inputLine;
					StringBuilder response = new StringBuilder();
					while ((inputLine = in.readLine()) != null) {
						response.append(inputLine);
					}
					in.close();
					response.toString();

					JsonElement jelement = new JsonParser().parse(response.toString());
					JsonObject jobject = jelement.getAsJsonObject();
					JsonArray features = jobject.getAsJsonArray("features");

					for (int i = 0; i < features.size(); i++) {

						JsonObject feature = (JsonObject) features.get(i);

						JsonObject geometry = (JsonObject) feature.get("geometry");
						JsonObject properties = (JsonObject) feature.get("properties");

						while (resultsetCategories.next()) {

							String fieldCategory = resultsetCategories.getString("field");
							String valueCategory = resultsetCategories.getString("value") != null
									? resultsetCategories.getString("value").toString()
									: "";

							try {
								String fieldValue = properties.get(fieldCategory).getAsString();
								if (fieldValue.equals(valueCategory)) {

									String sqlRestrictions = "SELECT r.* FROM vu.restrictions r, vu.category_restriction cr WHERE r.id = cr.id_restriction AND cr.id_category = "
											+ resultsetCategories.getString("id") + ";";

									ResultSet resultsetRestrictions = conn.getResultSetFromSql(sqlRestrictions);
									/**/

									String format = ",\"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"EPSG:3857\"}} }";
									String geometryString = geometry.toString().trim();
									geometryString = geometryString.substring(0, geometryString.length() - 1) + format;

									String sqlInterset = "select poligono_creado , "
											+ "st_area(st_intersection(poligono_creado, ST_Transform(ST_GeomFromGeoJSON('"
											+ geometryString
											+ "'), 3116))) as cruce, st_area(poligono_creado) as area from canutalito.terreno t "
											+ "where t.t_id = " + id + ";";

									ResultSet resultsetIntersect = connRDM.getResultSetFromSql(sqlInterset);
									resultsetIntersect.next();

									Float cruce = Float.parseFloat(resultsetIntersect.getString("cruce"));
									Float area = Float.parseFloat(resultsetIntersect.getString("area"));
									DecimalFormat frmt = new DecimalFormat("###.##");
									if (cruce > 0) {
										while (resultsetRestrictions.next()) {
											JsonObject afectaciones = new JsonObject();
											afectaciones.addProperty("area", frmt.format(area));
											afectaciones.addProperty("objeto", resultsetRestrictions.getString("name"));
											afectaciones.addProperty("proportion",
													frmt.format((cruce / area) * 100) + "%");
											afectaciones.addProperty("t_id", resultsetRestrictions.getString("id"));
											restrictions.add(afectaciones);
										}
									}

								}
							} catch (Exception e) {

							}

						}

					}

				}

			}

			conn.disconnect();
			connRDM.disconnect();

			return restrictions.toString();

			/*
			 * Postgres conn = new Postgres(); conn.connect(this.connectionString,
			 * this.connectionUser, this.connectionPassword, this.classForName); sql =
			 * "select json_agg( json_build_object('t_id', t_id, 'objeto', CASE WHEN identificador='1' THEN 'Ronda Hidrica' WHEN identificador='2' THEN 'Area Protegida' WHEN identificador='3' THEN 'Desarrollo Restringido POT' WHEN identificador='4' THEN 'Zona Militar' END, 'area', st_area(st_intersection(geometria, (select poligono_creado from "
			 * + this.rdmSchema + ".terreno where t_id= " + id +
			 * "))), 'proportion', (st_area(st_intersection(geometria, (select poligono_creado from "
			 * + this.rdmSchema + ".terreno where t_id= " + id +
			 * ")))/st_area((select poligono_creado from " + this.rdmSchema +
			 * ".terreno where t_id= " + id + "))) ) ) from " + this.rdmSchema +
			 * ".zona_homogenea_fisica WHERE ST_Intersects(geometria, (select poligono_creado from "
			 * + this.rdmSchema + ".terreno where t_id= " + id + "))=TRUE"; String response
			 * = conn.queryToString(sql); conn.disconnect(); if (response != null) return
			 * response; else return "{\"error\":\"No se encontraron registros.\"}";
			 */
		} catch (Exception e) {
			e.printStackTrace();
			return "{\"error\":\"" + e.getMessage() + "\"}";
		}
	}

	@GetMapping(value = "/public/parcel/cadastralcode", produces = { "application/json" })
	public String getCadastralCode(@RequestParam(required = false) Integer id) {
		String sql = "";
		try {
			Postgres conn = new Postgres();
			conn.connect(this.connectionString, this.connectionUser, this.connectionPassword, this.classForName);
			sql = "select json_build_object('numero_predial',p.numero_predial) from " + this.rdmSchema
					+ ".terreno t JOIN " + this.rdmSchema + ".uebaunit u ON t.t_id=u.ue_terreno JOIN " + this.rdmSchema
					+ ".predio p ON u.baunit_predio = p.t_id where t.t_id = " + id;
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