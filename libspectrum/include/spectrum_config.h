/*
   Copyright (c) 2016, 2023, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef ___SPECTRUM_CONFIG_INCLUDED___
#define ___SPECTRUM_CONFIG_INCLUDED___

#define MAX_NODE_ID_LENGTH 256

enum node_role_enum {
   COMPUTE,
   STORAGE_PRIMARY,
   STORAGE_REPLICA
};

struct node_config_t {
   std::string id;
   node_role_enum role;
   std::string address;
};

extern int spectrum_config_init();
extern node_config_t* find_current_node_config();
extern node_config_t* find_node_config_by_id(char* id);
extern node_config_t* find_storage_primary_node_config();
extern node_config_t* find_storage_replica_node_config();

#endif