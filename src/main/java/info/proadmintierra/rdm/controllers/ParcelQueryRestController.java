package info.proadmintierra.rdm.controllers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.text.DecimalFormat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
	@Value("${databaseVU.datasource}")
	private String databaseVUDatasource;
	@Value("${databaseVU.datasource.username}")
	private String databaseVUUsername;
	@Value("${databaseVU.datasource.password}")
	private String databaseVUPassword;

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
			//connRDM.disconnect();

			return restrictions.toString();

		} catch (Exception e) {
			e.printStackTrace();
			return "{\"error\":\"" + e.getMessage() + "\"}";
		}
	}

}