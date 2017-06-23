/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "hdfspp/hdfspp.h"
#include "common/uri.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"

namespace orc {

  class HdfsFileInputStream : public InputStream {
  private:
    std::string filename;
    std::unique_ptr<hdfs::FileHandle> file;
    std::unique_ptr<hdfs::FileSystem> file_system;
    uint64_t totalLength;

  public:
    HdfsFileInputStream(std::string _filename) {
      filename = _filename ;

      //Building a URI object from the given uri_path
      hdfs::optional<hdfs::URI> uri = hdfs::URI::parse_from_string(filename);
      if (!uri) {
        throw ParseError("Malformed URI: " + filename);
      }

      hdfs::Options options;
      //Setting the config path to the default: "$HADOOP_CONF_DIR" or "/etc/hadoop/conf"
      hdfs::ConfigurationLoader loader;
      //Loading default config files core-site.xml and hdfs-site.xml from the config path
      hdfs::optional<hdfs::HdfsConfiguration> config = loader.LoadDefaultResources<hdfs::HdfsConfiguration>();
      //TODO: HDFS-9539 - after this is resolved, valid config will always be returned.
      if(config){
        //Loading options from the config
        options = config->GetOptions();
      }
      hdfs::IoService * io_service = hdfs::IoService::New();
      //Wrapping file_system into a unique pointer to guarantee deletion
      file_system = std::unique_ptr<hdfs::FileSystem>(hdfs::FileSystem::New(io_service, "", options));
      if (!file_system) {
        throw ParseError("Can't create FileSystem object. ");
      }
      hdfs::Status status;
      //Check if the user supplied the host
      if(!uri.value().get_host().empty()){
        //If port is supplied we use it, otherwise we use the empty string so that it will be looked up in configs.
        std::string port = (uri.value().get_port()) ? std::to_string(uri.value().get_port().value()) : "";
        status = file_system->Connect(uri.value().get_host(), port);
        if (!status.ok()) {
          throw ParseError("Can't connect to " + uri.value().get_host() + ":" + port + ". " + status.ToString());
        }
      } else {
        status = file_system->ConnectToDefaultFs();
        if (!status.ok()) {
          if(!options.defaultFS.get_host().empty()){
            throw ParseError("Error connecting to " + options.defaultFS.str() + ". " + status.ToString());
          } else {
            throw ParseError("Error connecting to the cluster: defaultFS is empty. " + status.ToString());
          }
        }
      }

      if (!file_system) {
        throw ParseError("Can't connect the file system. ");
      }

      hdfs::FileHandle *file_raw = nullptr;
      status = file_system->Open(uri->get_path(), &file_raw);
      if (!status.ok()) {
        throw ParseError("Can't open " + uri->get_path() + ". " + status.ToString());
      }
      //wrapping file_raw into a unique pointer to guarantee deletion
      file.reset(file_raw);

      hdfs::StatInfo stat_info;
      status = file_system->GetFileInfo(uri->get_path(), stat_info);
      if (!status.ok()) {
        throw ParseError("Can't stat " + uri->get_path() + ". " + status.ToString());
      }
      totalLength = stat_info.length;
    }

    ~HdfsFileInputStream();

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 1048576; //1 MB
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {

      size_t last_bytes_read = 0;

      if (!buf) {
        throw ParseError("Buffer is null");
      }

      hdfs::Status status;
      status = file->PositionRead(buf, static_cast<size_t>(length), static_cast<off_t>(offset), &last_bytes_read);
      if(!status.ok()) {
        throw ParseError("Error reading the file: " + status.ToString());
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  };

  HdfsFileInputStream::~HdfsFileInputStream() {
  }

  std::unique_ptr<InputStream> readHdfsFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new HdfsFileInputStream(path));
  }
}
