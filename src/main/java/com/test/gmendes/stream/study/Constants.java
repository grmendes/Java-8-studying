package com.test.gmendes.stream.study;

/**
 * Class for keeping constants. Not best approach, but useful.
 *
 * @author grmendes
 */
public class Constants {

    public static final String SEPARATOR = ",";
    public static final String TXT_EXTENSION = ".txt";
    public static final String LAYOUT_FILE_BASE_NAME = String.format("layout%s", TXT_EXTENSION);
    public static final String NULL = "NULL";
    public static final String LAYOUT_HEADER = "Coluna,Tamanho,Inicio,Fim,Tipo";

    private Constants() {
        // Private default constructor.
    }
}
