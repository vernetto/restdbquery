@Path("/endpoint")

@Stateless

public class Endpoint {

    @GET

    @Produces(MediaType.APPLICATION_JSON)

    @Path("/query")

    public Response start(@QueryParam("ds") String datasource, @QueryParam("q") String query) {

        try {

            DataSource ds = lookupDataSource(datasource);

            try (Connection conn = ds.getConnection()) {

                try (Statement statement = conn.createStatement()) {

                    try (ResultSet rs = statement.executeQuery(query)) {

                        if(rs != null) {

                            ResultSetMetaData meta = rs.getMetaData();

                            StringBuilder sb = new StringBuilder();

                            sb.append("{");

                            // columns

                            sb.append("\"columns\":[");

                            for(int i = 1; i <= meta.getColumnCount(); i++) {

                                if(i > 1) {

                                    sb.append(",");

                                }

                                sb.append("\"").append(meta.getColumnLabel(i)).append("\"");

                            }

                            sb.append("]");

                           // data

                            sb.append(",");

                            sb.append("\"rows\":[");

                            int row = 1;

                            while(rs.next()) {

                                if(row > 1) {

                                    sb.append(",");

                                }

                                sb.append("{\"r\":").append(row).append(",\"d\":[");

                                for(int j = 1; j <= meta.getColumnCount(); j++) {

                                    if(j > 1) {

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

                        }

                        else {

                            return Response.noContent().build();

                        }

                    }

                }

            }

        }

        catch (Exception ex) {

            return Response.serverError().entity(ex.getMessage()).build();

        }

    }

 

    private DataSource lookupDataSource(String name) throws NamingException {

        return (DataSource) new InitialContext().lookup(name);

    }

}
