package com.pierre.query.rest;

import javax.ejb.Stateless;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

@Path("/endpoint")
@Stateless

public class EndPoint {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/query")
    public Response start(@QueryParam("ds") String datasource, @QueryParam("q") String query) {
        try {
            DataSource ds = lookupDataSource(datasource);
            try (Connection conn = ds.getConnection()) {
                try (Statement statement = conn.createStatement()) {
                    try (ResultSet rs = statement.executeQuery(query)) {
                        if (rs != null) {
                            ResultSetMetaData meta = rs.getMetaData();
                            StringBuilder sb = new StringBuilder();
                            sb.append("{");
                            // columns
                            sb.append("\"columns\":[");
                            for (int i = 1; i <= meta.getColumnCount(); i++) {
                                if (i > 1) {
                                    sb.append(",");
                                }
                                sb.append("\"").append(meta.getColumnLabel(i)).append("\"");
                            }
                            sb.append("]");
                            // data
                            sb.append(",");
                            sb.append("\"rows\":[");
                            int row = 1;
                            while (rs.next()) {
                                if (row > 1) {
                                    sb.append(",");
                                }
                                sb.append("{\"r\":").append(row).append(",\"d\":[");
                                for (int j = 1; j <= meta.getColumnCount(); j++) {
                                    if (j > 1) {
                                        sb.append(",");
                                    }
                                    sb.append("\"").append(rs.getObject(j)).append("\"");
                                }
                                sb.append("]}");
                                ++row;
                            }
                            sb.append("]");
                            sb.append("}");
                            return Response.ok().entity(sb.toString()).build();
                        } else {
                            return Response.noContent().build();
                        }
                    }
                }
            }
        } catch (Exception ex) {
            return Response.serverError().entity(ex.getMessage()).build();
        }
    }

    private DataSource lookupDataSource(String name) throws NamingException {
        return (DataSource) new InitialContext().lookup(name);
    }
}
