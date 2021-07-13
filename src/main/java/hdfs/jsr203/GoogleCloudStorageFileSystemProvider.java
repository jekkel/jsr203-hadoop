/*
 * Copyright 2016 Damien Carol <damien.carol@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
 package hdfs.jsr203;

/**
 * A file system provider for accessing Google Cloud Storage through hdfs
 * client. Only supports the <code>gs</code> scheme.
 *
 * @author Jörg Eichhorn {@literal <joerg.eichhorn@kiwigrid.com>}
 */
public class GoogleCloudStorageFileSystemProvider extends HadoopFileSystemProvider {

    public static final String SCHEME = "gs";

    @Override
    public String getScheme() {
        return SCHEME;
    }
}
