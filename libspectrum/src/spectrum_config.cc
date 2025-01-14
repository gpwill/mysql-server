/*****************************************************************************

Copyright (c) 2000, 2024, Oracle and/or its affiliates.
Copyright (c) 2008, 2009 Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2012, Facebook Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is designed to work with certain software (including
but not limited to OpenSSL) that is licensed under separate terms,
as designated in a particular file or component or in included license
documentation.  The authors of MySQL hereby grant you an additional
permission to link the program and your derivative works with the
separately licensed software that they have either included with
the program or referenced in the documentation.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#include <vector>

#include "spectrum_config.h"

std::vector<node_config_t> nodes;

int spectrum_config_init() {
  nodes.push_back({"compute-0", node_role_enum::COMPUTE, "localhost:3306"});
  nodes.push_back({"compute-1", node_role_enum::COMPUTE, "localhost:3307"});
  nodes.push_back({"storage-0", node_role_enum::STORAGE_REPLICA, "localhost:64000"});
  nodes.push_back({"storage-1", node_role_enum::STORAGE_PRIMARY, "localhost:64001"});
  return 0;
}

node_config_t* find_current_node_config() {
  char* node_id = getenv("SPECTRUM_NODE_ID");
  if (!node_id) return nullptr;

  return find_node_config_by_id(node_id);
}

node_config_t* find_node_config_by_id(char* id) {
  for (node_config_t& node : nodes) {
    if (!strcmp(node.id.c_str(), id)) {
      return &node;
    }
  }
  return nullptr;
}

node_config_t* find_storage_primary_node_config() {
  for (node_config_t& node : nodes) {
    if (node.role == node_role_enum::STORAGE_PRIMARY) {
      return &node;
    }
  }
  return nullptr;
}

node_config_t* find_storage_replica_node_config() {
  for (node_config_t& node : nodes) {
    if (node.role == node_role_enum::STORAGE_REPLICA) {
      return &node;
    }
  }
  return nullptr;
}

