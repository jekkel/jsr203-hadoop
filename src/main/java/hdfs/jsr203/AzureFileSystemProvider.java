package hdfs.jsr203;

/**
 * created on 14.12.16.
 *
 * @author Jörg Eichhorn {@literal <joerg.eichhorn@kiwigrid.com>}
 */
public class AzureFileSystemProvider extends HadoopFileSystemProvider {

    public static final String SCHEME = "wasb";

    @Override
    public String getScheme() {
        return SCHEME;
    }
}
