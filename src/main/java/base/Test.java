package base;

import java.text.SimpleDateFormat;

/**
 * <p/>
 * <li>Description:</li>
 * <li>@author: lee </li>
 * <li>Date: 2017/10/23 </li>
 * <li>@version: 1.0.0 </li>
 */
public class Test {

    public static void main ( String[] args ) {

        SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        long currentTimeMillis = System.currentTimeMillis();
        System.out.println(currentTimeMillis);
        String currentTime = sdf.format( currentTimeMillis );
        System.out.println(currentTime);
    }
}
