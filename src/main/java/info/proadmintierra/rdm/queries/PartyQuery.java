package info.proadmintierra.rdm.queries;


/**
 * BasicQuery
 */
public class PartyQuery {

    public static String getQuery(String schema, String parcel_fmi, String parcel_number, String nupre) {

        String query = "SELECT array_to_json(array_agg(row_to_json(t))) from" +
            "(select "+
            "    i.t_id, "+
            "    i.nombre, "+
            "    i.razon_social, "+
            "    i.tipo, "+
            "    i.tipo_interesado_juridico, "+
            "    i.tipo_documento, "+
            "    i.documento_identidad, "+
            "    d.comienzo_vida_util_version, "+
            "    d.tipo, "+
            "    100 as participacion "+
            "from "+ 
            "({schema}.predio p join {schema}.col_derecho d on p.t_id=d.unidad_predio)  "+
            "join {schema}.col_interesado i on d.interesado_col_interesado=i.t_id "+
            "where p.fmi='{parcel_fmi}' OR p.nupre='{nupre}' or p.numero_predial='{parcel_number}' "+
            "union "+
            "select "+
            "    i.t_id, "+
            "    i.nombre, "+
            "    i.razon_social, "+
            "    i.tipo, "+
            "    i.tipo_interesado_juridico, "+
            "    i.tipo_documento, "+
            "    i.documento_identidad, "+
            "    d.comienzo_vida_util_version, "+
            "    d.tipo, "+
            "    f.numerador*100/f.denominador as participacion "+
            "from  "+
            "({schema}.predio p join {schema}.col_derecho d on p.t_id=d.unidad_predio)  "+
            "join {schema}.la_agrupacion_interesados ai on d.interesado_la_agrupacion_interesados = ai.t_id  "+
            "join {schema}.miembros m on ai.t_id = m.agrupacion "+
            "join {schema}.col_interesado i on i.t_id=m.interesados_col_interesado "+
            "join {schema}.fraccion f on m.t_id=f.miembros_participacion "+
            "where p.fmi='{parcel_fmi}' OR p.nupre='{nupre}' or p.numero_predial='{parcel_number}') t";

        query = query.replaceAll("\\{schema\\}", schema);
        query = query.replaceAll("\\{parcel_fmi\\}", parcel_fmi != null ? parcel_fmi : "NULL");
        query = query.replaceAll("\\{parcel_number\\}", parcel_number != null ? parcel_number : "NULL");
        query = query.replaceAll("\\{nupre\\}", nupre != null ? nupre : "NULL");

        return query;
    }
}