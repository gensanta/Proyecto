package es.schemeupdater;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class SchemeUpdater {

    final static String TABLA = "ainfges";
    final static String COLINFORME = "\"AIGG-hnuminf\"";
    final static String COLLINEA = "\"AIGG-hnumlin\"";
    final static String COLCOLUMNA = "\"AIGG-hnumcol\"";
    final static String COLLITERAL = "\"AIGG-llitera\"";
    final static String COLCODEDE = "\"AIGG-ecodede\"";

    String tokenCSV = "|";
    boolean imprimirPorSalidaStandard = false;
    String nombreFichero = "esquema.csv";

    boolean esCodigoDoble = false;
    String[] codigosDobles = new String[2];
    int iPosDoble = -1;

    static SchemeUpdater a = new SchemeUpdater();
    private String cadena = "jdbc:postgresql://localhost:5432/postgres";
    private String usuario = "postgres";
    private String contraseña = "postgres";

    public static void main(String[] args) {
        System.out.println(
                "   _____  ______ __  __ ______ __  ___ ______    \r\n" +
                        "  / ___/ / ____// / / // ____//  |/  // ____/    \r\n" +
                        "  \\__ \\ / /    / /_/ // __/  / /|_/ // __/       \r\n" +
                        " ___/ // /___ / __  // /___ / /  / // /___       \r\n" +
                        "/____/ \\____//_/ /_//_____//_/  /_//_____/       \r\n" +
                        "   __  __ ____   ____   ___   ______ ______ ____ \r\n" +
                        "  / / / // __ \\ / __ \\ /   | /_  __// ____// __ \\\r\n" +
                        " / / / // /_/ // / / // /| |  / /  / __/  / /_/ /\r\n" +
                        "/ /_/ // ____// /_/ // ___ | / /  / /___ / _, _/ \r\n" +
                        "\\____//_/    /_____//_/  |_|/_/  /_____//_/ |_|  \r\n" +
                        "                                                 ");

        if (args.length == 0) {
            imprimeAyuda();
            return;
        }


        if (args.length > 0) {
            if (args[0].equals("?")) {
                imprimeAyuda();
                return;
            } else a.tokenCSV = args[0];
        }
        if (args.length > 1) {
            a.nombreFichero = args[1];
        }
        if (args.length > 2) {
            a.imprimirPorSalidaStandard = (args[2].toUpperCase().equals("S"));
        }
        // cadena de conexion
        if (args.length > 3) {
            a.cadena = args[3];
        }
        // usuario
        if (args.length > 4) {
            a.usuario = args[4];
        }
        // contraseña
        if (args.length > 5) {
            a.contraseña = args[5];
        }

        Connection con = a.conectarPSG();

        if (con != null) {
            System.out.println("conectado!");
            Map<String, String> esquema = a.proceso(con);

            // si se han obtenido datos
            if (esquema != null && esquema.size()> 0) {
                if (a.imprimirPorSalidaStandard) {
                    System.out.println("******************************************************************************");
                    System.out.println("******************************************************************************");
                    System.out.println("******************************************************************************");
                    System.out.println("******************************************************************************");
                    System.out.println("******************************************************************************");
                    System.out.println("******************************************************************************");
                }
                generarFichero(esquema);
            }

            cerrar(con);
        }

    }

    private static void generarFichero(Map<String, String> esquema) {

        String path = System.getProperty("user.home") + File.separator + "Documents";
        File fichero = new File(path + "/" + a.nombreFichero);

        try {
            FileWriter fw = new FileWriter(fichero);
            esquema.values().stream().forEach(v -> {
                try {
                    tratarFila(v,fw);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            fw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void imprimeAyuda() {
        System.out.println("Parametros disponibles:");
        System.out.println("un parametro: ?             -> imprime ayuda");
        System.out.println("              otro caracter -> delimitador del csv (por defecto |)");
        System.out.println("2do parametro -> nombre (solo nombre) del fichero con ruta (por defecto esquema.csv). Se guardará en la carpeta Documentos del usuario.");
        System.out.println("3er parametro -> si = 'S', sacar por consola el fichero (por defecto, N).");
        System.out.println("4o  parametro -> cadena de conexión (por defecto jdbc:postgresql://localhost:5432/postgres).");
        System.out.println("5o  parametro -> usuario de bbdd    (por defecto postgres).");
        System.out.println("6o  parametro -> contraseña         (por defecto postgres).");

        System.out.println(" Si se invoca sin parametros, creará o actualizará el fichero esquema.csv en la carpeta Documentos del usuario actual, utilizando como token '|' y sin imprimir por consola el fichero.");
    }

    private static void tratarFila(String v, FileWriter fw) throws IOException {

        if (a.imprimirPorSalidaStandard ) System.out.println(v);
        fw.write(v + "\n");

    }

    private Map<String, String> proceso(Connection con) {
        Map<String, String> resultado = new HashMap<String, String>();
        try {
            //SELECT "AIGG-llitera" FROM ainfges ORDER BY "AIGG-hnuminf","AIGG-hnumlin","AIGG-hnumcol"
            Statement st = con.createStatement();
            String sql = "SELECT " + COLINFORME + " AS INF," +COLLINEA +" AS LIN," + COLCOLUMNA + " AS COL," + COLLITERAL + " AS LIT," + COLCODEDE + " AS CODEDE "
                    + " FROM " + TABLA
//					+ " WHERE "+ COLINFORME + " >900 "
                    + " ORDER BY " + COLINFORME + "," + COLLINEA + "," + COLCOLUMNA;
            ResultSet rs = ejecutarConsulta(st,sql);

            // campos a leer en cada fila
            Integer inf   =  null;
            Integer lin   =  null;
            Integer col   =  null;
            String lit    =  null;
            String codede =  null;

            // variables de trabajo
            String[] codAct  = new String[2];

            // en las filas N puede haber dos variables y dos descripciones
            String[] desc = new String[2];
            String clave = null;

            int numeroDeCodigosEnLinea;

            while (rs.next()) {
                inf    =  rs.getInt("INF");
                lin    =  rs.getInt("LIN");
                col    =  rs.getInt("COL");
                lit    =  rs.getString("LIT");
                codede =  rs.getString("CODEDE");

                // si sigue en la misma linea y hay codede, lo guarda
                if (clave != null && clave.equals(inf+":"+lin)) {

                    if (codede != null && !codede.trim().equals("")) {
                        guardaDatosEnResultado(resultado, inf, lin, col, codede, codAct, desc);
                    }
                    if (esCodigoDoble) {
                        iPosDoble++;
                    }
                } else 	if (lit.trim().length()> 0) {
                    // reinicializa
                    numeroDeCodigosEnLinea = 0;
                    codAct[0] = null;
                    codAct[1] = null;
                    desc[0] = null;
                    desc[1] = null;

                    // analiza
                    StringTokenizer to = new StringTokenizer(lit,"!");
                    while(to.hasMoreTokens()) {
                        String sAux = to.nextToken().trim();
                        if (esCodigo(sAux, false)) {
                            codAct[numeroDeCodigosEnLinea] = sAux;
                            if (to.hasMoreTokens()) {
                                desc[numeroDeCodigosEnLinea] = to.nextToken().trim();
                                clave = inf+":"+lin;

                                guardaDescripcionEnResultado(resultado, inf, lin, col, codAct[numeroDeCodigosEnLinea], desc[numeroDeCodigosEnLinea]);
                            }
                            numeroDeCodigosEnLinea++;
                        }

                    }
                }
            }
            if (clave != null) {
                guardaDatosEnResultado(resultado, inf, lin, col, codede, codAct, desc);
            }

            cerrar(rs);
            cerrar(st);

            if (resultado.size() > 0) {
                resultado = resultado.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue, LinkedHashMap::new));
            }

        } catch (SQLException e) {
            System.out.println("oE");
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return resultado;
    }

    private void guardaDatosEnResultado(Map<String, String> resultado, Integer inf, Integer lin, Integer col,
                                        String codede, String[] codAct, String[] desc) {

        // toma de tierra
        if (codAct[0]==null) return;

        // calculo de indice de entradas con dos variables en la misma fila 0 y dos descripciones
        int indice = codAct[1] == null ? 0 : (col == 1 ? 0 : 1);

        if (!esCodigoDoble) {
            String registro = inf + tokenCSV + lin + tokenCSV + col + tokenCSV + codAct[indice]+ tokenCSV +desc[indice] + tokenCSV + codede;
            if (imprimirPorSalidaStandard) System.out.println(registro);
            resultado.put(inf + adaptaCodigo(codAct[indice]) + lin+col, registro);
        } else {
            // calcula numero de variable

            int numVariable = (iPosDoble < (codAct[indice].startsWith("J") ? 3 : 2 )) ? 0 : (iPosDoble < 4 ) ? 1: 2;
            // se incrementa en el proceso 	iPosDoble;
            // puede que no haya primera variable
            if (codigosDobles[numVariable] != null) {
                String registro = inf + tokenCSV + lin + tokenCSV + col + tokenCSV + codigosDobles[numVariable]+ tokenCSV +desc[indice] + tokenCSV + codede;
                if (imprimirPorSalidaStandard) System.out.println(registro);
                resultado.put(inf + adaptaCodigo(codigosDobles[numVariable]) + lin+col, registro);
            }
        }
    }


    private void guardaDescripcionEnResultado(Map<String, String> resultado, Integer inf, Integer lin, Integer col,	String codAct, String desc) {
        // si es un codigo doble
        if (codAct.contains("/")) {
            // guarda un registro por cada codigo
            StringTokenizer st = new StringTokenizer(codAct,"/");

            while (st.hasMoreTokens()) {
                String s = st.nextToken();
                imprimeRegistroIndividualDeDescripcion(resultado, inf, lin, col, s, desc);
            }
        } else {
            // sino, guarda un solo registro
            imprimeRegistroIndividualDeDescripcion(resultado, inf, lin, col, codAct, desc);
        }
    }

    private void imprimeRegistroIndividualDeDescripcion(Map<String, String> resultado, Integer inf, Integer lin, Integer col, String codAct, String desc) {
        String registro = inf + tokenCSV + lin + tokenCSV + col + tokenCSV + codAct + tokenCSV + desc + tokenCSV;
        if (imprimirPorSalidaStandard) System.out.println(registro);
        resultado.put(inf + adaptaCodigo(codAct) + lin + col, registro);
    }


    private String adaptaCodigo(String codAct) {
        String resultado = null;

        // si es empieza con un numero, no hay que hacer nada
        if (Character.isDigit(codAct.charAt(0)))
            resultado = codAct;
        else {
            String sAux = codAct.substring(1);
            // si el codigo tiene dos digitos despues de la letra, nada que hacer
            resultado = (sAux.length()==2) ? codAct : codAct.substring(0,1) + "0" + sAux;
        }

        return resultado;
    }


    // comprueba si es una letra y un numero de uno o dos digitos
    private boolean esCodigoSimple(String cad, boolean letraYNumeros) {
        boolean resultado = false;
        if (cad.length() == 2 || cad.length() == 3 ) {
            String sLetra = cad.substring(0, 1);
            String resto = cad.substring(1);

            resultado = (letraYNumeros ? sLetra.matches("[A-Z]") : true) &&  resto.matches("[0-9]+");
        }

        // si es codigo simple, limpia los datos del codigo doble
        if (resultado) {
            codigosDobles[0] = null;
            codigosDobles[1] = null;;
            esCodigoDoble = false;
        }

        return resultado;
    }

    private boolean esCodigo(String cad, boolean letraYNumeros) {
        boolean resultado = esCodigoSimple(cad, letraYNumeros);

        // es una secuencia de variables separadas por /
        if (!resultado &&  cad.contains("/") ) {
            if (cad.length() > 7) {
                StringTokenizer st = new StringTokenizer(cad," ");
                cad = st.nextToken();
            }
            // si la cedena contiene // es que es despreciable
            if (!cad.contains("//")) {

                StringTokenizer st = new StringTokenizer(cad,"/");
                boolean bAux = true;
                String[] saCodigos = new String[2];
                int i = 0;
                while(bAux && st.hasMoreTokens()) {
                    String sAux = st.nextToken();
                    bAux = esCodigoSimple(sAux, letraYNumeros);
                    // si es un codigo, lo anota
                    if (bAux) saCodigos[i++]=sAux;
                }

                // si es un doble codigo
                if (bAux && i>0 ) {
                    codigosDobles[0] = saCodigos[0];
                    codigosDobles[1] = saCodigos[1];
                    esCodigoDoble = true;
                    iPosDoble     =0;
                    resultado     = true;
                } else {
                    codigosDobles[0] = null;
                    codigosDobles[1] = null;;
                    esCodigoDoble = false;
                    resultado     = false;
                }
            }

        }

        return resultado;
    }

    /*******************************************************************************************/

    public Connection conectarPSG() {

        Connection con = null;

        try {
            Class.forName("org.postgresql.Driver");

            con = DriverManager.getConnection(cadena, usuario, contraseña);
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT VERSION()");

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException ex) {
            System.out.println("PETE AL CONECTAR;" + ex);
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("PETE AL CONECTAR;" + e);
            e.printStackTrace();
        }
        return con;
    }

    //Ejecuta un SELECT y devuelve el Resultset con los resultados
    public ResultSet ejecutarConsulta(Statement st, String cadSQ) throws SQLException
    {
        ResultSet rs = null;

        try
        {
            rs = st.executeQuery(cadSQ);
            return rs;
        }
        catch(SQLException sqlex)
        {
            sqlex.printStackTrace();
            return rs;
        }

    }
    public Statement conectar() {
        try {
            String controlador = "sun.jdbc.odbc.JdbcOdbcDriver";
            Class.forName(controlador).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            System.out.println("Error al cargar el Controlador " + e);
        }
        try {
            String DSN;
            DSN = "jdbc:odbc:audi";

            Connection conexion = DriverManager.getConnection(DSN);
            Statement st = conexion.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            return st;
        } catch (SQLException e) {
            System.out.println("Error al realizar la conexion " + e);
        }
        return null;
    }

    // Cierra un objeto Resultset
    public static void cerrar(ResultSet rs)
    {
        try
        {
            rs.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }

    // cierra un objeto Statemet
    public static void cerrar(Statement st)
    {
        try
        {
            st.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }

    // Cierra un objeto Connection
    public static void cerrar(Connection con)
    {
        try
        {
            con.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }
}
