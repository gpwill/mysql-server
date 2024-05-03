# Copyright (c) 2006, 2023, Oracle and/or its affiliates.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

include_directories(${Protobuf_INCLUDE_DIRS})

MYSQL_ADD_PLUGIN(storage_agent
  daemon_example.cc spectrum.grpc.pb.h spectrum.grpc.pb.cc spectrum.pb.h spectrum.pb.cc
  LINK_LIBRARIES utf8_range::utf8_range utf8_range::utf8_validity protobuf::libprotobuf gRPC::grpc++_reflection gRPC::grpc++ absl::log absl::flags absl::flags_parse absl::check
  DEFAULT
  MODULE_OUTPUT_NAME "libstorage_agent"
  )

target_compile_features(storage_agent PUBLIC cxx_std_17)

INSTALL(FILES
  daemon_example.ini
  DESTINATION ${INSTALL_PLUGINDIR}
  COMPONENT Test
  )
