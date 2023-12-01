package org.example;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: Wukun
 * @Date: 2023/11/29/11:23
 */
public class MBTilesGeneratorTest {

    @Test
    public void test() throws SQLException {
        MBTilesGenerator.diskToMBTiles("testdata","testdata.mbtiles","png","zyx",true);

        //MBTilesGenerator.mbtilesMetadataToDisk("test.mbtiles");

        //MBTilesGenerator.mbtilesToDisk("test.mbtiles","png","testdata","zyx",null);

    }
}
