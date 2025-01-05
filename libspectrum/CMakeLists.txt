# Copyright (c) 2021, 2024, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

SET(SPECTRUM_BASE_SRC_DIR ${CMAKE_SOURCE_DIR}/libspectrum/src)
SET(SPECTRUM_PB_PROTO_FILES ${SPECTRUM_BASE_SRC_DIR}/protobuf/spectrum.proto)
SET(SPECTRUM_PB_GENERATE_DIR ${CMAKE_BINARY_DIR}/libspectrum/generated/protobuf)

FILE(MAKE_DIRECTORY ${SPECTRUM_PB_GENERATE_DIR})

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/libspectrum/include
  ${SPECTRUM_PB_GENERATE_DIR}
)

MYSQL_PROTOBUF_GENERATE_CPP_LIBRARY(
    spectrum_pb
    ${SPECTRUM_PB_PROTO_FILES}
    OUTPUT_DIRECTORY ${SPECTRUM_PB_GENERATE_DIR}
    GRPC_OUTPUT_DIRECTORY ${SPECTRUM_PB_GENERATE_DIR}
)
TARGET_LINK_LIBRARIES(spectrum_pb PUBLIC ext::libprotobuf grpc++_unsecure)

ADD_STATIC_LIBRARY(spectrum_common
  ${SPECTRUM_BASE_SRC_DIR}/spectrum_config.cc
  ${SPECTRUM_BASE_SRC_DIR}/spectrum_common.cc
  LINK_LIBRARIES spectrum_pb
)

ADD_STATIC_LIBRARY(spectrum_log
  ${SPECTRUM_BASE_SRC_DIR}/spectrum_log.cc
  LINK_LIBRARIES spectrum_pb
)

ADD_STATIC_LIBRARY(spectrum_compute
  ${SPECTRUM_BASE_SRC_DIR}/spectrum_compute.cc
  LINK_LIBRARIES spectrum_pb spectrum_common
)

ADD_STATIC_LIBRARY(spectrum_storage
  ${SPECTRUM_BASE_SRC_DIR}/spectrum_storage.cc
  LINK_LIBRARIES spectrum_pb spectrum_common spectrum_log
)