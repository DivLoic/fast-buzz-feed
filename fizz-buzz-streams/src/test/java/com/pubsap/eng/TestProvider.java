package com.pubsap.eng;

import java.io.File;
import java.util.Objects;

/**
 * Created by loicmdivad.
 */
public class TestProvider {

    public static File fileFromResource(String name) {
        return new File(getPathFromResources(name));
    }

    public static String getPathFromResources(String filename) {
        return Objects
                .requireNonNull(
                        TestProvider
                                .class
                                .getClassLoader()
                                .getResource(filename)
                ).getFile();
    }
}
