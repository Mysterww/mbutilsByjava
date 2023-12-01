package org.example;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class MBTilesGenerator {

    private static int flipY(int zoom, int y) {
        return (int) (Math.pow(2, zoom) - 1) - y;
    }

    private static void createMBTilesTables(String databasePath) {
        String url = "jdbc:sqlite:" + databasePath;

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);");
            stmt.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);");

            stmt.execute("CREATE TABLE metadata (name TEXT, value TEXT);");
            stmt.execute("CREATE UNIQUE INDEX name ON metadata (name);");


            stmt.execute("CREATE TABLE grids (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, grid BLOB);");

            stmt.execute("CREATE TABLE grid_data (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, key_name TEXT, key_json TEXT);");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static Connection connectToMBTiles(String mbtilesFile) {
        String url = "jdbc:sqlite:" + mbtilesFile;
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return connection;
    }

    private static void optimizeConnection(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            statement.execute("PRAGMA synchronous=0");
            statement.execute("PRAGMA locking_mode=EXCLUSIVE");
            statement.execute("PRAGMA journal_mode=DELETE");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void compressionPrepare(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS images (tile_data blob, tile_id integer)");
            statement.execute("CREATE TABLE IF NOT EXISTS map (zoom_level integer, tile_column integer, tile_row integer, tile_id integer)");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void optimizeDatabase(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            statement.execute("ANALYZE;");
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            statement.execute("VACUUM;");
            //恢复默认隔离等级
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void compressionDo(Connection connection, int chunk) {
        try {
            Statement statement = connection.createStatement();
            int overlapping = 0;
            int unique = 0;
            int total = 0;

            ResultSet totalTiles = statement.executeQuery("SELECT COUNT(zoom_level) FROM tiles");
            int totalTilesCount = totalTiles.getInt(1);
            int lastId = 0;
            for (int i = 0; i < (totalTilesCount / chunk + 1); i++) {
                List<String> files = new ArrayList<>();
                List<Integer> ids = new ArrayList<>();
                PreparedStatement selectStatement = connection.prepareStatement("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE rowid > ? AND rowid <= ?");
                selectStatement.setInt(1, i * chunk);
                selectStatement.setInt(2, (i + 1) * chunk);
                ResultSet rows = selectStatement.executeQuery();
                while (rows.next()) {
                    total++;
                    byte[] tileData = rows.getBytes("tile_data");
                    String tileDataString = new String(tileData);
                    if (files.contains(tileDataString)) {
                        overlapping++;
                        PreparedStatement insertMapStatement = connection.prepareStatement("INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?)");
                        insertMapStatement.setInt(1, rows.getInt("zoom_level"));
                        insertMapStatement.setInt(2, rows.getInt("tile_column"));
                        insertMapStatement.setInt(3, rows.getInt("tile_row"));
                        insertMapStatement.setInt(4, ids.get(files.indexOf(tileDataString)));
                        insertMapStatement.executeUpdate();
                    } else {
                        unique++;
                        lastId++;
                        ids.add(lastId);
                        files.add(tileDataString);
                        PreparedStatement insertImagesStatement = connection.prepareStatement("INSERT INTO images (tile_id, tile_data) VALUES (?, ?)");
                        insertImagesStatement.setInt(1, lastId);
                        insertImagesStatement.setBytes(2, tileData);
                        insertImagesStatement.executeUpdate();
                        PreparedStatement insertMapStatement = connection.prepareStatement("INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?, ?, ?, ?)");
                        insertMapStatement.setInt(1, rows.getInt("zoom_level"));
                        insertMapStatement.setInt(2, rows.getInt("tile_column"));
                        insertMapStatement.setInt(3, rows.getInt("tile_row"));
                        insertMapStatement.setInt(4, lastId);
                        insertMapStatement.executeUpdate();
                    }
                }

                rows.close();
                selectStatement.close();
            }

            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void compressionFinalize(Connection connection) {
        try {
            Statement statement = connection.createStatement();

            statement.executeUpdate("DROP TABLE IF EXISTS tiles");
            statement.executeUpdate("CREATE VIEW tiles AS SELECT map.zoom_level AS zoom_level, map.tile_column AS tile_column, map.tile_row AS tile_row, images.tile_data AS tile_data FROM map JOIN images ON images.tile_id = map.tile_id");

            statement.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS map_index ON map (zoom_level, tile_column, tile_row)");
            statement.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS images_id ON images (tile_id)");


            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            statement.executeUpdate("VACUUM");
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); // reset default value of isolation level

            statement.executeUpdate("ANALYZE");

            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<String> getDirs(String path) {
        List<String> directories = new ArrayList<>();
        File dir = new File(path);

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        directories.add(file.getName());
                    }
                }
            }
        } else {
            throw new RuntimeException("指定目录不存在");
        }

        return directories;
    }

    private static byte[] compressData(String data) throws IOException {
        Deflater deflater = new Deflater();
        byte[] input = data.getBytes("UTF-8");

        deflater.setInput(input);
        deflater.finish();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(input.length);
        byte[] buffer = new byte[1024];

        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }

        deflater.end();
        outputStream.close();

        return outputStream.toByteArray();
    }

    private static String decompressToJSON(byte[] compressedData) {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedData);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedData.length);
        byte[] buffer = new byte[1024];

        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
        } catch (DataFormatException e) {
            e.printStackTrace();
        } finally {
            inflater.end();
        }

        return outputStream.toString();
    }

    public static void diskToMBTiles(String directoryPath, String mbtilesFile, String format, String scheme, boolean compression) throws SQLException {
        Connection con = null;
        try {
            con = connectToMBTiles(mbtilesFile);
            Statement stmt = con.createStatement();
            optimizeConnection(con);
            createMBTilesTables(mbtilesFile);
            String imageFormat = format != null ? format : "png";

            try {
                byte[] metabytes = Files.readAllBytes(Paths.get(directoryPath, "metadata.json"));
                String metaString = new String(metabytes);
                JSONObject metadata = JSONObject.parseObject(metaString);
                imageFormat = format != null ? format : imageFormat;
                for (String name : metadata.keySet()) {
                    String value = metadata.getString(name);
                    stmt.executeUpdate("INSERT INTO metadata (name, value) VALUES ('" + name + "', '" + value + "')");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            int count = 0;
            long startTime = System.currentTimeMillis();

            List<String> dirs = getDirs(directoryPath);

            for (String dir : dirs)
            {
                int z=0,y=0,x=0;
                if (scheme.equals("ags"))
                {
                    z = Integer.parseInt(dir.replace("L", ""));
                }
                else if (scheme.equals("gwc"))
                {
                    z = Integer.parseInt(dir.substring(dir.length() - 2));
                }
                else
                {
                    z = Integer.parseInt(dir);
                }

                //获取目录中的子目录
                List<String> childDirectory = getDirs(directoryPath + File.separator + dir);

                if (childDirectory != null)
                {
                    for (String rowDir : childDirectory) {
                        if (scheme.equals("ags"))
                        {
                            y=flipY(z,Integer.parseInt(rowDir.replace("R", ""), 16));
                        }
                        else if (scheme.equals("gwc"))
                        {
                            ;
                        }
                        else if (scheme.equals("zyx"))
                        {
                            y=flipY(z,Integer.parseInt(rowDir));
                        }
                        else
                        {
                            x=Integer.parseInt(rowDir);
                        }

                        File directory = new File(directoryPath, dir + File.separator + rowDir);
                        File[] files = directory.listFiles();

                        for (File current_file : files)
                        {
                            //获取文件名和扩展名
                            String[] parts = current_file.getName().split("\\.", 2);
                            String file_name=parts[0];
                            String ext=(parts.length > 1) ? parts[1] : "";
                            //对文件进行读取
                            Path filePath = Paths.get(directoryPath, dir, rowDir, current_file.getName());

                            //文件内容
                            byte[] file_content = Files.readAllBytes(filePath);

                            if (scheme.equals("xyz"))
                            {
                                y=flipY(z,Integer.parseInt(file_name));
                            }
                            else if (scheme.equals("ags"))
                            {
                                x=Integer.parseInt(file_name.replace("C", ""), 16);
                            }
                            else if (scheme.equals("gwc"))
                            {
                                x=Integer.parseInt(file_name.split("_")[0]);
                                y=Integer.parseInt(file_name.split("_")[1]);
                            }
                            else if (scheme.equals("zyx"))
                            {
                                x=Integer.parseInt(file_name);
                            }
                            else
                            {
                                y=Integer.parseInt(file_name);
                            }

                            //图像处理
                            if (ext.equals(imageFormat))
                            {
                                String sql = "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)";
                                PreparedStatement pst = con.prepareStatement(sql);
                                pst.setInt(1,z);
                                pst.setInt(2,x);
                                pst.setInt(3,y);
                                pst.setBytes(4,file_content);
                                pst.executeUpdate();
                                pst.close();
                            }
                            else if (ext.equals("grid.json"))
                            {
                                String file_content_utf8 = new String(file_content, StandardCharsets.UTF_8);
                                Pattern pattern = Pattern.compile("[\\w\\s=+-/]+\\((\\{(.|\\n)*})\\);?");
                                Matcher matcher = pattern.matcher(file_content_utf8);
                                if (matcher.find()) {
                                    file_content_utf8 = matcher.group(1);
                                }

                                JSONObject utfgrid = JSONObject.parseObject(file_content_utf8);
                                JSONObject data = utfgrid.getJSONObject("data");
                                utfgrid.remove("data");

                                byte[] compressbytes = compressData(utfgrid.toJSONString());

                                String sql = "INSERT INTO grids (zoom_level, tile_column, tile_row, grid) VALUES (?, ?, ?, ?)";
                                PreparedStatement pst = con.prepareStatement(sql);
                                pst.setInt(1,z);
                                pst.setInt(2,x);
                                pst.setInt(3,y);
                                pst.setBytes(4,compressbytes);

                                pst.executeUpdate();
                                pst.close();

                                ArrayList<String> gridKeys = new ArrayList<>();
                                for (String key : utfgrid.keySet()) {
                                    if (!key.isEmpty())
                                    {
                                        gridKeys.add(key);
                                    }
                                }

                                for (String key_name : gridKeys)
                                {
                                    JSONObject keyJson = data.getJSONObject(key_name);

                                    String insertSql = "INSERT INTO grid_data (zoom_level, tile_column, tile_row, key_name, key_json) VALUES (?, ?, ?, ?, ?)";

                                    PreparedStatement pst2 = con.prepareStatement(insertSql);

                                    pst2.setInt(1,z);
                                    pst2.setInt(2,x);
                                    pst2.setInt(3,y);
                                    pst2.setString(4,key_name);
                                    pst2.setString(5,keyJson.toJSONString());
                                    pst2.executeUpdate();
                                }

                            }

                        }

                    }
                }

            }

            if (compression) {
                compressionPrepare(con);
                compressionDo(con, 256);
                compressionFinalize(con);
            }

            optimizeDatabase(con);

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void mbtilesMetadataToDisk(String mbtilesFile) {
        Connection con = null;
        try {
            con = connectToMBTiles(mbtilesFile);
            Statement stmt = con.createStatement();
            JSONObject metadata = new JSONObject();
            ResultSet rs = stmt.executeQuery("SELECT name, value FROM metadata");
            while (rs.next()) {
                String name = rs.getString("name");
                String value = rs.getString("value");
                metadata.put(name, value);
            }
            rs.close();
            try (PrintWriter out = new PrintWriter("metadata.json")) {
                out.println(metadata.toJSONString());
            }

        } catch (SQLException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void mbtilesToDisk(String mbtilesFile,String format, String directoryPath,String scheme,String callbackarg) {
        Connection con = null;
        try {
            con = connectToMBTiles(mbtilesFile);
            Statement stmt = con.createStatement();
            JSONObject metadata = new JSONObject();

            Path directory_path = Paths.get(directoryPath);
            Files.createDirectories(directory_path);

            //将元数据写出到metadata.json
            ResultSet rs = stmt.executeQuery("SELECT name, value FROM metadata");
            while (rs.next()) {
                String name = rs.getString("name");
                String value = rs.getString("value");
                metadata.put(name, value);
            }
            rs.close();

            try (FileWriter file = new FileWriter(directory_path.resolve("metadata.json").toString())) {
                file.write(metadata.toJSONString());
            }

            //获取zoom_level列的行数
            int count=0;
            ResultSet resultSet = stmt.executeQuery("select count(zoom_level) from tiles");
            if (resultSet.next())
            {
                count=resultSet.getInt(1);
            }



            int done = 0;

            //将formatter写入layer.json
            JSONObject formatter = metadata.getJSONObject("formatter");
            if (formatter!=null)
            {
                JSONObject formatterJson = new JSONObject();
                formatterJson.put("formatter",formatter);
                FileWriter fileWriter = new FileWriter(directory_path.resolve("layer.json").toString());
                fileWriter.write(formatterJson.toJSONString());
                fileWriter.close();
            }


            //处理层级数据
            ResultSet tiles = stmt.executeQuery("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles");
            while (tiles.next()) {
                int z = tiles.getInt("zoom_level");
                int x = tiles.getInt("tile_column");
                int y = tiles.getInt("tile_row");

                //对坐标进行转换

                Path tile_dir;
                if (scheme.equals("xyz"))
                {
                    y=flipY(z,y);
                    tile_dir=directory_path.resolve(String.valueOf(z)).resolve(String.valueOf(x));
                }
                else if (scheme.equals("wms"))
                {
                    tile_dir = Paths.get(directoryPath,
                            String.format("%02d", z),
                            String.format("%03d", x / 1000000),
                            String.format("%03d", (x / 1000) % 1000),
                            String.format("%03d", x % 1000),
                            String.format("%03d", y / 1000000),
                            String.format("%03d", (y / 1000) % 1000));
                }
                else
                {
                    tile_dir=directory_path.resolve(String.valueOf(z)).resolve(String.valueOf(x));
                }

                //如果不存在则创建该目录
                if (!Files.isDirectory(tile_dir))
                {
                    Files.createDirectories(tile_dir);
                }

                Path tile;
                if (scheme.equals("wms"))
                {
                    tile = Paths.get(String.format("%s/%03d.%s", tile_dir, y % 1000, format != null ? format : "png"));
                }
                else
                {
                    tile=Paths.get(String.format("%s/%d.%s", tile_dir, y, format != null ? format : "png"));
                }

                byte[] tileData1 = tiles.getBytes("tile_data");
                Files.write(tile,tileData1);
                done++;



            }

            //对grid格式进行处理
            String callback=callbackarg;
            done=0;

            ResultSet countResult = stmt.executeQuery("select count(zoom_level) from grids;");
            count=0;
            if (countResult.next()) {
                count = countResult.getInt(1);
            }

            // 获取 grids 表中的数据
            ResultSet gridsResult = stmt.executeQuery("select zoom_level, tile_column, tile_row, grid from grids;");
            while (gridsResult.next()) {
                int zoomLevel = gridsResult.getInt("zoom_level");
                int tileColumn = gridsResult.getInt("tile_column");
                int y = gridsResult.getInt("tile_row");
                // 获取 grid 的数据，这里假设 grid 的数据类型为 Blob 或 byte[]
                byte[] gridData = gridsResult.getBytes("grid");
                // 处理 grid 数据

                String query = "SELECT key_name, key_json FROM grid_data WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?";
                PreparedStatement pstmt = con.prepareStatement(query);
                // 设置查询参数
                pstmt.setInt(1, zoomLevel); // 假设 zoomLevel 是一个变量，代表要查询的 zoom_level
                pstmt.setInt(2, tileColumn); // 假设 tileColumn 是一个变量，代表要查询的 tile_column
                pstmt.setInt(3, y); // 假设 tileRow 是一个变量，代表要查询的 tile_row

                ResultSet gridDataResult = pstmt.executeQuery();



                if (scheme.equals("xyz"))
                {
                    y=flipY(zoomLevel,y);
                }
                Path grid_dir=directory_path.resolve(String.valueOf(zoomLevel)).resolve(String.valueOf(tileColumn));

                if (!Files.isDirectory(grid_dir))
                {
                    Files.createDirectories(grid_dir);
                }

                Path grid = grid_dir.resolve(y + ".grid.json");

                String grid_json = decompressToJSON(gridData);

                JSONObject grid_json_obj = JSONObject.parseObject(grid_json);

                // 处理结果集
                Map<String, Object> data = new HashMap<>();
                while (gridDataResult.next()) {
                    String keyName = gridDataResult.getString("key_name");
                    String keyJson = gridDataResult.getString("key_json");
                    // 处理获取到的数据
                    data.put(keyName,JSONObject.parseObject(keyJson));
                }

                grid_json_obj.put("data",data);

                if (callback==null)
                {
                    Files.write(grid,JSON.toJSONString(grid_json_obj).getBytes(), StandardOpenOption.CREATE);
                }
                else
                {
                    String content = callback + "(" + JSON.toJSONString(grid_json_obj) + ");";
                    Files.write(grid,content.getBytes());
                }
                done++;

            }

        } catch (SQLException | IOException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if (con != null) {
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
